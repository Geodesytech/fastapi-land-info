from typing import Union, List
import arcpy, json, datetime, uuid
from pydantic import BaseModel
from fastapi import Depends, FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os, base64
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, Response, JSONResponse
from starlette.requests import Request

load_dotenv()

app = FastAPI(title="SAP Integration")

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Set workspace
output_path= r"C:\Stuff\DR-AKSHAY\SAP-Integration\DB\GDB"
gdb_filename = "land_info.gdb"
gdb_path = "{}\\{}".format(output_path, gdb_filename)
arcpy.env.workspace = gdb_path

sapFC = "SAPInfo"
ownerFC = "OwnerInfo"
rnrFC = "RnRComponents"
rnrPaymentFC = "OwnerPaymentHistory"
govtFC = "GovtComponents"
govtPaymentFC = "GovtPaymentHistory"
fraFC = "FraComponents"
fraPaymentFC = "FraPaymentHistory"
land_table = {"pvt" : rnrPaymentFC, "govt": fraPaymentFC, "fra" : govtPaymentFC}
sapFields = ["project_id", "check_list", "fiscal_year","company_code", "vendor", "sp_g_l","witholding_tax_code", "withholding_tax_type", "tax_code", "business_place", "section_code", "business_area", "payment_ref","profit_center", "assignment", "text", "bank_partner_type", "doc_sub_category", "remark","land_act" ]

users_db = {
    "adani_gisportal": {
        "username": "adani_gisportal",
        "full_name": "Adani Gisportal",
        "email": "girish@test.com",
        "hashed_password": "Wxn96p36WQoeG6Lruj3vjPGga31lW",
        # "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",
        "disabled": False,
    }
}


security = HTTPBasic()

class User(BaseModel):
    username: str
    email: str = None
    full_name: str = None
    disabled: bool = None

class UserInDB(User):
    hashed_password: str

class PaymentUpdate(BaseModel):
    drp_doc_no: str #DPR doc no.
    payment_doc_no: str #Payment doc no.
    payment_date: str #Payment date
    Unique_Id: str
    Text: str

class SAPInfo(BaseModel):
    project_id: str
    check_list: str
    fiscal_year: str
    company_code: str
    vendor: str
    sp_g_l: str
    witholding_tax_code: str
    withholding_tax_type: str
    tax_code: str
    business_place: str
    section_code: str
    business_area: str
    payment_ref: str
    profit_center: str
    assignment: str
    text: str
    bank_partner_type: str
    doc_sub_category: str
    remark: str
    land_act: str

class ParcelId(BaseModel):
    parcel_id: str
    project_id: str

class InitRnR(BaseModel):
    parcel_id: str
    init_land_compn_p: float
    init_assets_com_p: float
    init_tree_com_p: float
    init_interest: float
    init_pay_cost_unit: float
    init_pay_cost_plot: float
    init_pay_cost_lieu: float
    init_grant_dis_fam: float
    init_grant_scst_fam: float
    init_tran_cost_fam: float
    init_pay_cattle_sh: float
    init_grant_art_trader: float
    init_settle_allow: float
    init_fee_stamp_duty: float
    init_pay_cost_unit_wbs: str
    init_pay_cost_plot_wbs: str
    init_pay_cost_lieu_wbs: str
    init_grant_dis_fam_wbs: str
    init_grant_scst_fam_wbs: str
    init_tran_cost_fam_wbs: str
    init_pay_cattle_sh_wbs: str
    init_grant_art_trader_wbs: str
    init_settle_allow_wbs: str
    init_fee_stamp_duty_wbs: str
    init_land_compn_p_wbs: str
    init_assets_com_p_wbs: str
    init_tree_com_p_wbs: str
    init_interest_wbs: str

class PayComponents(BaseModel):
    amount: float
    compn_type: str
    WBS: str

class InitGovt(BaseModel):
    parcel_id: str
    init_pay_premium: float
    init_pay_lease_rent_an: float
    init_pay_assets: float
    init_pay_ench_gov_lnd: float
    init_pay_ench_forest_lnd: float
    init_pay_premium_wbs: str
    init_pay_lease_rent_an_wbs: str
    init_pay_assets_wbs: str
    init_pay_ench_gov_lnd_wbs: str
    init_pay_ench_forest_lnd_wbs: str
    init_lease_duedate: str

class InitFra(BaseModel):
    parcel_id: str
    init_pay_npv: float
    init_pay_safety_zn: float
    init_amt_wlmp: float
    init_amt_ca_scheme: float
    init_pay_gap_smc: float
    init_pay_tree_fel: float
    init_pay_npv_wbs: str
    init_pay_safety_zn_wbs: str
    init_amt_wlmp_wbs: str
    init_amt_ca_scheme_wbs: str
    init_pay_gap_smc_wbs: str
    init_pay_tree_fel_wbs: str
    init_land_compn_f: float
    init_assets_com_f: float
    init_tree_com_f: float
    init_land_compn_f_wbs: str
    init_assets_com_f_wbs: str
    init_tree_com_f_wbs: str

class PaymentRnR(BaseModel):
    parcel_id: str
    owner_id: str
    payment_detail: List[PayComponents]
    sap_list: List[SAPInfo]

class PaymentFra(BaseModel):
    parcel_id: str
    payment_detail: List[PayComponents]
    sap_list: List[SAPInfo]

class PaymentGovt(BaseModel):
    parcel_id: str
    payment_detail: List[PayComponents]
    sap_list: List[SAPInfo]

class PaymentHistory(BaseModel):
    parcel_id: str
    owner_id: str


@app.get("/test")
def read_root():
    return {"Hello": "World"}

## GET API: To get list of all owner
## Params: parcel_id
## Response: [{ OwnerID, OwnerName, Percentage }]
@app.post("/rnr/")
async def get_rnr( parcel_id: ParcelId):
    print(parcel_id.parcel_id)
    SQL = "parcel_id = '{}'".format(parcel_id.parcel_id)
    sapSQL = "project_id = '{}'".format(parcel_id.project_id)
    ownerList, sapList, rnrList, totalComp, totalComPaid = [], [], [], 0, 0
    paymentFields = [ "amount", "compn_type"]
    ownerFields = ["parcel_id", "owner_id", "owner_name", "percentage", "exception", "ex_fields"]
    rnrFields = ["parcel_id", "pay_cost_unit", "pay_cost_plot", "pay_cost_lieu", "grant_dis_fam", "grant_scst_fam", "tran_cost_fam",
                "pay_cattle_sh", "grant_art_trader", "settle_allow", "fee_stamp_duty",
                "pay_cost_unit_wbs", "pay_cost_plot_wbs", "pay_cost_lieu_wbs", "grant_dis_fam_wbs", "grant_scst_fam_wbs",
                "tran_cost_fam_wbs", "pay_cattle_sh_wbs", "grant_art_trader_wbs", "settle_allow_wbs", "fee_stamp_duty_wbs",
                "land_compn_p", "assets_com_p", "tree_com_p",
                "land_compn_p_wbs", "assets_com_p_wbs", "tree_com_p_wbs", "interest","interest_wbs"]

    with arcpy.da.SearchCursor(ownerFC, ownerFields,  SQL) as cursor:
        # parcelList = [row for row in cursor]
        for row in cursor:
            ownerList.append({"parcel_id": row[0], "owner_id":row[1], "owner_name": row[2], "percentage":row[3], "exception": row[4], "ex_fields": row[5]})
        del cursor, row

    with arcpy.da.SearchCursor(sapFC, sapFields,  sapSQL) as sapCursor:
        # parcelList = [row for row in sapCursor]
        for row in sapCursor:
            sapList.append({
                "project_id": row[0],
                "check_list": row[1],
                "fiscal_year": row[2],
                "company_code": row[3],
                "vendor": row[4],
                "sp_g_l": row[5],
                "witholding_tax_code": row[6],
                "withholding_tax_type": row[7],
                "tax_code": row[8],
                "business_place": row[9],
                "section_code": row[10],
                "business_area": row[11],
                "payment_ref": row[12],
                "profit_center": row[13],
                "assignment": row[14],
                "text": row[15],
                "bank_partner_type": row[16],
                "doc_sub_category": row[17],
                "remark": row[18],
                "land_act" : row[19]
            })
        del sapCursor

    ## Get all the result RnR components
    with arcpy.da.SearchCursor(rnrFC, rnrFields,  SQL) as rnrCursor:
        for row in rnrCursor:
            totalComp = row[1]+row[2]+row[3]+row[4]+row[5]+row[6]+row[7]+row[8]+row[9]+row[10]+row[21]+row[22]+row[23]+row[27]
            rnrList.append({
                "parcel_id": row[0],
                "pay_cost_unit": row[1],
                "pay_cost_plot": row[2],
                "pay_cost_lieu": row[3],
                "grant_dis_fam": row[4],
                "grant_scst_fam": row[5],
                "tran_cost_fam": row[6],
                "pay_cattle_sh": row[7],
                "grant_art_trader": row[8],
                "settle_allow": row[9],
                "fee_stamp_duty": row[10],
                "pay_cost_unit_wbs": row[11],
                "pay_cost_plot_wbs": row[12],
                "pay_cost_lieu_wbs": row[13],
                "grant_dis_fam_wbs": row[14],
                "grant_scst_fam_wbs": row[15],
                "tran_cost_fam_wbs": row[16],
                "pay_cattle_sh_wbs": row[17],
                "grant_art_trader_wbs": row[18],
                "settle_allow_wbs": row[19],
                "fee_stamp_duty_wbs": row[20],
                "land_compn_p": row[21],
                "assets_com_p": row[22],
                "tree_com_p": row[23],
                "land_compn_p_wbs": row[24],
                "assets_com_p_wbs": row[25],
                "tree_com_p_wbs": row[26],
                "interest": row[27],
                "interest_wbs": row[28]
            })
        del rnrCursor
    with arcpy.da.SearchCursor(rnrPaymentFC, paymentFields, SQL) as cursor:
        for row in cursor:
            totalComPaid += row[0]
    return {"ownerList": ownerList, "sapList": sapList, "rnrList": rnrList, "totalComp":totalComp, "totalComPaid": totalComPaid}

def create_tran_id(compType: str, landType: str):
    id = str(uuid.uuid4())[0:30]
    parcel_id = "{};{};{}".format(id, compType, landType)
    return parcel_id

def send_request_sap(sap_attrib):
    PASS = os.getenv('PASS')
    USER = os.getenv('USER')
    SAP_PAY_URL = os.getenv('SAP_PAY_URL')
    auth=HTTPBasicAuth(USER, PASS)


    response = requests.post(SAP_PAY_URL,auth=auth, json=sap_attrib)

    print('Response Code : ' + str(response.status_code))

    if response.status_code == 200:
        print('Login Successfully: '+ response.text)
        return {"isSuccess": True}
    return {"isSuccess": False}


def verify_password(plain_password, hashed_password):
    # return pwd_context.verify(plain_password, hashed_password)
    return plain_password == hashed_password

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

def authenticate_user(fake_db, username: str, password: str):
    user = get_user(fake_db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user

@app.post("/init/rnr/")
async def post_init_rnr( init_rnr: InitRnR):
    # print(init_rnr)
    # return tuple(init_rnr)
    # SQL = "parcel_id = '{}'".format(parcel_id.parcel_id)
    # ownerList, sapList, rnrList = [], [], []
    rnrFields = ["parcel_id",
    "pay_cost_unit",
    "pay_cost_plot",
    "pay_cost_lieu",
    "grant_dis_fam",
    "grant_scst_fam",
    "tran_cost_fam",
    "pay_cattle_sh",
    "grant_art_trader",
    "settle_allow",
    "fee_stamp_duty",
    "pay_cost_unit_wbs",
    "pay_cost_plot_wbs",
    "pay_cost_lieu_wbs",
    "grant_dis_fam_wbs",
    "grant_scst_fam_wbs",
    "tran_cost_fam_wbs",
    "pay_cattle_sh_wbs",
    "grant_art_trader_wbs",
    "settle_allow_wbs",
    "fee_stamp_duty_wbs",
    "land_compn_p",
    "assets_com_p",
    "tree_com_p",
    "land_compn_p_wbs",
    "assets_com_p_wbs",
    "tree_com_p_wbs",
    "interest",
    "interest_wbs"]

    # parcel_id, init_pay_cost_unit, init_pay_cost_plot, init_pvt_interest_wbs = init_rnr
    rowRecord = (
    init_rnr.parcel_id,
    init_rnr.init_pay_cost_unit,
    init_rnr.init_pay_cost_plot,
    init_rnr.init_pay_cost_lieu,
    init_rnr.init_grant_dis_fam,
    init_rnr.init_grant_scst_fam,
    init_rnr.init_tran_cost_fam,
    init_rnr.init_pay_cattle_sh,
    init_rnr.init_grant_art_trader,
    init_rnr.init_settle_allow,
    init_rnr.init_fee_stamp_duty,
    init_rnr.init_pay_cost_unit_wbs,
    init_rnr.init_pay_cost_plot_wbs,
    init_rnr.init_pay_cost_lieu_wbs,
    init_rnr.init_grant_dis_fam_wbs,
    init_rnr.init_grant_scst_fam_wbs,
    init_rnr.init_tran_cost_fam_wbs,
    init_rnr.init_pay_cattle_sh_wbs,
    init_rnr.init_grant_art_trader_wbs,
    init_rnr.init_settle_allow_wbs,
    init_rnr.init_fee_stamp_duty_wbs,
    init_rnr.init_land_compn_p,
    init_rnr.init_assets_com_p,
    init_rnr.init_tree_com_p,
    init_rnr.init_land_compn_p_wbs,
    init_rnr.init_assets_com_p_wbs,
    init_rnr.init_tree_com_p_wbs,
    init_rnr.init_interest,
    init_rnr.init_interest_wbs
    )
    print(rowRecord)
    with arcpy.da.InsertCursor(rnrFC, rnrFields) as tbList:
        tbList.insertRow(rowRecord)
        del tbList
    return {"msg": "Required payment updated succefully"}

@app.post("/payment/rnr/")
async def post_payment_rnr( payment_rnr: PaymentRnR):
    print(payment_rnr)
    sap_list = payment_rnr.sap_list
    sap_attrib = {
        "Unique_Id" : payment_rnr.parcel_id,
        "Checklist_Doc_Type": sap_list[0].check_list,
        "Fiscal_Year": sap_list[0].fiscal_year,
        "Company_Code": sap_list[0].company_code,
        "Vendor": sap_list[0].vendor,
        "Sp_GL": sap_list[0].sp_g_l,
        "Withholding_Tax_Type": sap_list[0].withholding_tax_type,
        "Witholding_Tax_Code": sap_list[0].witholding_tax_code,
        "Document_Sub_Category": sap_list[0].doc_sub_category,
        "Remarks": sap_list[0].remark
    }
    i = 0
    items = []
    for sap_pay_detail in payment_rnr.payment_detail:
        i += 1
        items.append({
            "Amount": sap_pay_detail.amount,
            "Tax_Code": sap_list[0].tax_code,
            "Business_Place": sap_list[0].business_place,
            "Section_Code": sap_list[0].section_code,
            "Business_Area": sap_list[0].business_area,
            "Due_On": datetime.date.today(),
            "Payment_Reference": sap_list[0].payment_ref,
            "Profit_Center": sap_list[0].profit_center,
            "WBS_Element": sap_pay_detail.WBS,
            "Assignment": sap_list[0].assignment,
            "Text": "{}".format(create_tran_id(sap_pay_detail.compn_type, "pvt")),
            "Bank_Partner_Type": sap_list[0].bank_partner_type
        })
    sap_attrib["Item"] = items

    result = send_request_sap(sap_attrib)
    if not result.isSuccess:
        return {"msg": "SAP system not valiate the payment"}
    paymentHistoryFields = ["parcel_id", "owner_id", "amount", "datetime", "compn_type", "WBS" ]

    with arcpy.da.InsertCursor(rnrPaymentFC, paymentHistoryFields) as tbList:
        for pay_detail in payment_rnr.payment_detail:
            rowRecord = (payment_rnr.parcel_id, payment_rnr.owner_id, pay_detail.amount, datetime.date.today(), pay_detail.compn_type, pay_detail.WBS)
            tbList.insertRow(rowRecord)
        del tbList
    return {"msg": "Payment raised succefully"}

@app.post("/payment/history/rnr/")
async def get_payment_history( payment_history: PaymentHistory):
    print(payment_history)
    expression = "parcel_id = '{}' AND owner_id = '{}'".format(payment_history.parcel_id, payment_history.owner_id)
    paymentHistory = {}
    totalComPaid = 0
    ownerFields = [ "amount", "compn_type"]
    with arcpy.da.SearchCursor(rnrPaymentFC, ownerFields, expression) as cursor:
        for row in cursor:
            if row[1] in paymentHistory:
                paymentHistory[row[1]] += row[0]
            else:
                paymentHistory[row[1]] = row[0]
            totalComPaid += row[0]

        del cursor
    print(paymentHistory, len(paymentHistory))
    return {"isSuccess": not len(paymentHistory)==0, "totalComPaid": totalComPaid, "paymentHistory": paymentHistory,"parcel_id": payment_history.parcel_id, "owner_id": payment_history.owner_id }


## GOVT
@app.post("/init/govt/")
async def post_init_govt( init_govt: InitGovt):
    govtFields = ["parcel_id", "pay_premium", "pay_lease_rent_an", "pay_assets", "pay_ench_gov_lnd",
                  "pay_ench_forest_lnd", "pay_premium_wbs", "pay_lease_rent_an_wbs", "pay_assets_wbs",
                  "pay_ench_gov_lnd_wbs", "pay_ench_forest_lnd_wbs", "lease_duedate"]

    rowRecord = (init_govt.parcel_id, init_govt.init_pay_premium, init_govt.init_pay_lease_rent_an,
                 init_govt.init_pay_assets, init_govt.init_pay_ench_gov_lnd, init_govt.init_pay_ench_forest_lnd,
                 init_govt.init_pay_premium_wbs, init_govt.init_pay_lease_rent_an_wbs, init_govt.init_pay_assets_wbs,
                 init_govt.init_pay_ench_gov_lnd_wbs, init_govt.init_pay_ench_forest_lnd_wbs,init_govt.init_lease_duedate)
    print(rowRecord)
    with arcpy.da.InsertCursor(govtFC, govtFields) as tbList:
        tbList.insertRow(rowRecord)
        del tbList
    return {"msg": "Required payment updated succefully"}

## POST API: To get list of all GOVT payment components
## Params: parcel_id
## Response: [{ OwnerID, OwnerName, Percentage }]
@app.post("/govt/")
async def get_govt( parcel_id: ParcelId):
    print(parcel_id.parcel_id)
    SQL = "parcel_id = '{}'".format(parcel_id.parcel_id)
    sapSQL = "project_id = '{}'".format(parcel_id.project_id)
    sapList, govtList = [], []
    paymentHistory = {}
    totalComPaid, totalComp = 0, 0
    govtFields = ["parcel_id", "pay_premium", "pay_lease_rent_an", "pay_assets", "pay_ench_gov_lnd",
                  "pay_ench_forest_lnd", "pay_premium_wbs", "pay_lease_rent_an_wbs", "pay_assets_wbs",
                  "pay_ench_gov_lnd_wbs", "pay_ench_forest_lnd_wbs", "lease_duedate"]
    govtPaymentFields = [ "amount", "compn_type"]

    with arcpy.da.SearchCursor(sapFC, sapFields,  sapSQL) as sapCursor:
        # parcelList = [row for row in sapCursor]
        for row in sapCursor:
            sapList.append({
                "project_id": row[0],
                "check_list": row[1],
                "fiscal_year": row[2],
                "company_code": row[3],
                "vendor": row[4],
                "sp_g_l": row[5],
                "witholding_tax_code": row[6],
                "withholding_tax_type": row[7],
                "tax_code": row[8],
                "business_place": row[9],
                "section_code": row[10],
                "business_area": row[11],
                "payment_ref": row[12],
                "profit_center": row[13],
                "assignment": row[14],
                "text": row[15],
                "bank_partner_type": row[16],
                "doc_sub_category": row[17],
                "remark": row[18],
                "land_act" : row[19]
            })
        del sapCursor

    with arcpy.da.SearchCursor(govtPaymentFC, govtPaymentFields, SQL) as cursor:
        for row in cursor:
            if row[1] in paymentHistory:
                paymentHistory[row[1]] += row[0]
            else:
                paymentHistory[row[1]] = row[0]
            totalComPaid += row[0]
        del cursor
    print(paymentHistory, len(paymentHistory))

    ## Get all the result GOVT components
    with arcpy.da.SearchCursor(govtFC, govtFields,  SQL) as govtCursor:
        for row in govtCursor:
            totalComp = row[1]+row[2]+row[3]+row[4]+row[5]
            govtList.append({
                "parcel_id": row[0],
                "pay_premium": row[1],
                "pay_lease_rent_an": row[2],
                "pay_assets": row[3],
                "pay_ench_gov_lnd": row[4],
                "pay_ench_forest_lnd": row[5],
                "pay_premium_wbs": row[6],
                "pay_lease_rent_an_wbs": row[7],
                "pay_assets_wbs": row[8],
                "pay_ench_gov_lnd_wbs":row[9],
                "pay_ench_forest_lnd_wbs": row[10],
                "lease_duedate":row[11]
            })
        del govtCursor
    return {"sapList": sapList, "totalComp":totalComp, "totalComPaid": totalComPaid, "govtList": govtList, "isSuccess": not len(paymentHistory)==0, "paymentHistory": paymentHistory}

## POST API: To post the payment raise by..
## Params: parcel_id
## Response: [{ OwnerID, OwnerName, Percentage }]
@app.post("/payment/govt/")
async def post_payment_govt( payment_govt: PaymentGovt):
    print(payment_govt)
    sap_list = payment_govt.sap_list
    sap_attrib = {
        "Unique_Id" : payment_govt.parcel_id,
        "Checklist_Doc_Type": sap_list[0].check_list,
        "Fiscal_Year": sap_list[0].fiscal_year,
        "Company_Code": sap_list[0].company_code,
        "Vendor": sap_list[0].vendor,
        "Sp_GL": sap_list[0].sp_g_l,
        "Withholding_Tax_Type": sap_list[0].withholding_tax_type,
        "Witholding_Tax_Code": sap_list[0].witholding_tax_code,
        "Document_Sub_Category": sap_list[0].doc_sub_category,
        "Remarks": sap_list[0].remark
    }
    i = 0
    items = []
    for sap_pay_detail in payment_govt.payment_detail:
        i += 1
        items.append({
            "Amount": sap_pay_detail.amount,
            "Tax_Code": sap_list[0].tax_code,
            "Business_Place": sap_list[0].business_place,
            "Section_Code": sap_list[0].section_code,
            "Business_Area": sap_list[0].business_area,
            "Due_On": datetime.date.today(),
            "Payment_Reference": sap_list[0].payment_ref,
            "Profit_Center": sap_list[0].profit_center,
            "WBS_Element": sap_pay_detail.WBS,
            "Assignment": sap_list[0].assignment,
            "Text": "{}".format(create_tran_id(sap_pay_detail.compn_type, "govt")),
            "Bank_Partner_Type": sap_list[0].bank_partner_type
        })
    sap_attrib["Item"] = items

    result = send_request_sap(sap_attrib)
    if not result.isSuccess:
        return {"msg": "SAP system not valiate the payment"}

    paymentHistoryFields = ["parcel_id", "amount", "datetime", "compn_type", "WBS" ]

    with arcpy.da.InsertCursor(govtPaymentFC, paymentHistoryFields) as tbList:
        for pay_detail in payment_govt.payment_detail:
            rowRecord = (payment_govt.parcel_id, pay_detail.amount, datetime.date.today(), pay_detail.compn_type, pay_detail.WBS)
            tbList.insertRow(rowRecord)
        del tbList
    return {"msg": "Payment raised succefully"}


## FOREST
@app.post("/init/fra/")
async def post_init_fra( init_fra: InitFra):
    fraFields = ["parcel_id", "pay_npv", "pay_safety_zn", "amt_wlmp", "amt_ca_scheme", "pay_gap_smc", "pay_tree_fel", "pay_npv_wbs", "pay_safety_zn_wbs", "amt_wlmp_wbs", "amt_ca_scheme_wbs", "pay_gap_smc_wbs", "pay_tree_fel_wbs",
     "land_compn_f", "assets_com_f", "tree_com_f", "land_compn_f_wbs", "assets_com_f_wbs", "tree_com_f_wbs"]

    rowRecord = (init_fra.parcel_id, init_fra.init_pay_npv, init_fra.init_pay_safety_zn, init_fra.init_amt_wlmp, init_fra.init_amt_ca_scheme, init_fra.init_pay_gap_smc, init_fra.init_pay_tree_fel, init_fra.init_pay_npv_wbs, init_fra.init_pay_safety_zn_wbs, init_fra.init_amt_wlmp_wbs, init_fra.init_amt_ca_scheme_wbs, init_fra.init_pay_gap_smc_wbs, init_fra.init_pay_tree_fel_wbs, init_fra.init_land_compn_f, init_fra.init_assets_com_f, init_fra.init_tree_com_f, init_fra.init_land_compn_f_wbs, init_fra.init_assets_com_f_wbs, init_fra.init_tree_com_f_wbs)
    print(rowRecord)
    with arcpy.da.InsertCursor(fraFC, fraFields) as tbList:
        tbList.insertRow(rowRecord)
        del tbList
    return {"msg": "Required payment updated succefully"}

## GET API: To get list of all Forest payment components
## Params: parcel_id
## Response: [{ OwnerID, OwnerName, Percentage }]
@app.post("/fra/")
async def get_fra( parcel_id: ParcelId):
    print(parcel_id.parcel_id)
    SQL = "parcel_id = '{}'".format(parcel_id.parcel_id)
    sapSQL = "project_id = '{}'".format(parcel_id.project_id)
    sapList, fraList = [], []
    paymentHistory = {}
    totalComPaid, totalComp = 0, 0
    fraFields = ["parcel_id", "pay_npv", "pay_safety_zn", "amt_wlmp", "amt_ca_scheme", "pay_gap_smc", "pay_tree_fel", "pay_npv_wbs", "pay_safety_zn_wbs", "amt_wlmp_wbs", "amt_ca_scheme_wbs", "pay_gap_smc_wbs", "pay_tree_fel_wbs",
     "land_compn_f", "assets_com_f", "tree_com_f", "land_compn_f_wbs", "assets_com_f_wbs", "tree_com_f_wbs"]
    fraPaymentFields = [ "amount", "compn_type"]

    with arcpy.da.SearchCursor(sapFC, sapFields,  sapSQL) as sapCursor:
        # parcelList = [row for row in sapCursor]
        for row in sapCursor:
            sapList.append({
                "project_id": row[0],
                "check_list": row[1],
                "fiscal_year": row[2],
                "company_code": row[3],
                "vendor": row[4],
                "sp_g_l": row[5],
                "witholding_tax_code": row[6],
                "withholding_tax_type": row[7],
                "tax_code": row[8],
                "business_place": row[9],
                "section_code": row[10],
                "business_area": row[11],
                "payment_ref": row[12],
                "profit_center": row[13],
                "assignment": row[14],
                "text": row[15],
                "bank_partner_type": row[16],
                "doc_sub_category": row[17],
                "remark": row[18],
                "land_act" : row[19]
            })
        del sapCursor

    with arcpy.da.SearchCursor(fraPaymentFC, fraPaymentFields, SQL) as cursor:
        for row in cursor:
            if row[1] in paymentHistory:
                paymentHistory[row[1]] += row[0]
            else:
                paymentHistory[row[1]] = row[0]
            totalComPaid  += row[0]

        del cursor
    print(paymentHistory, len(paymentHistory))

    ## Get all the result Forest components
    with arcpy.da.SearchCursor(fraFC, fraFields,  SQL) as fraCursor:
        for row in fraCursor:
            totalComp = row[1]+row[2]+row[3]+row[4]+row[5]+row[6]+row[13]+row[14]+row[15]
            fraList.append({
                "parcel_id": row[0],
                "pay_npv": row[1],
                "pay_safety_zn": row[2],
                "amt_wlmp": row[3],
                "amt_ca_scheme": row[4],
                "pay_gap_smc": row[5],
                "pay_tree_fel": row[6],
                "pay_npv_wbs": row[7],
                "pay_safety_zn_wbs": row[8],
                "amt_wlmp_wbs": row[9],
                "amt_ca_scheme_wbs": row[10],
                "pay_gap_smc_wbs": row[11],
                "pay_tree_fel_wbs": row[12],
                "land_compn_f": row[13],
                "assets_com_f": row[14],
                "tree_com_f": row[15],
                "land_compn_f_wbs": row[16],
                "assets_com_f_wbs": row[17],
                "tree_com_f_wbs": row[18]
            })
        del fraCursor
    return {"sapList": sapList, "totalComp": totalComp, "totalComPaid": totalComPaid, "fraList": fraList, "isSuccess": not len(paymentHistory)==0, "paymentHistory": paymentHistory}

## POST API: To post the payment raise by..
## Params: parcel_id
## Response: [{ OwnerID, OwnerName, Percentage }]
@app.post("/payment/fra/")
async def post_payment_fra( payment_fra: PaymentFra):
    print(payment_fra)
    sap_list = payment_fra.sap_list
    sap_attrib = {
        "Unique_Id" : payment_fra.parcel_id,
        "Checklist_Doc_Type": sap_list[0].check_list,
        "Fiscal_Year": sap_list[0].fiscal_year,
        "Company_Code": sap_list[0].company_code,
        "Vendor": sap_list[0].vendor,
        "Sp_GL": sap_list[0].sp_g_l,
        "Withholding_Tax_Type": sap_list[0].withholding_tax_type,
        "Witholding_Tax_Code": sap_list[0].witholding_tax_code,
        "Document_Sub_Category": sap_list[0].doc_sub_category,
        "Remarks": sap_list[0].remark
    }
    i = 0
    items = []
    for sap_pay_detail in payment_fra.payment_detail:
        i += 1
        items.append({
            "Amount": sap_pay_detail.amount,
            "Tax_Code": sap_list[0].tax_code,
            "Business_Place": sap_list[0].business_place,
            "Section_Code": sap_list[0].section_code,
            "Business_Area": sap_list[0].business_area,
            "Due_On": datetime.date.today(),
            "Payment_Reference": sap_list[0].payment_ref,
            "Profit_Center": sap_list[0].profit_center,
            "WBS_Element": sap_pay_detail.WBS,
            "Assignment": sap_list[0].assignment,
            "Text": "{}".format(create_tran_id(sap_pay_detail.compn_type, "fra")),
            "Bank_Partner_Type": sap_list[0].bank_partner_type
        })
    sap_attrib["Item"] = items

    result = send_request_sap(sap_attrib)
    if not result.isSuccess:
        return {"msg": "SAP system not valiate the payment"}

    paymentHistoryFields = ["parcel_id", "amount", "datetime", "compn_type", "WBS" ]

    with arcpy.da.InsertCursor(fraPaymentFC, paymentHistoryFields) as tbList:
        for pay_detail in payment_fra.payment_detail:
            rowRecord = (payment_fra.parcel_id, pay_detail.amount, datetime.date.today(), pay_detail.compn_type, pay_detail.WBS)
            tbList.insertRow(rowRecord)
        del tbList
    return {"msg": "Payment raised succefully"}

@app.post("/login")
async def login_basic(auth: HTTPBasicCredentials = Depends(security),nitem: str = Body(embed=True)):
    if not auth:
        # return {"msg": "401 unauthorize"}
        response = Response(headers={"WWW-Authenticate": "Basic"}, status_code=401, content="Incorrect email or password")
        return response

    try:
        print(auth)
        # decoded = base64.b64decode(auth).decode("ascii")
        # username, _, password = decoded.partition(":")
        user = authenticate_user(users_db, auth.username, auth.password)
        if not user:
            raise HTTPException(status_code=400, detail="Incorrect email or password")

        return { "msg": "success"}
    except:
        response = Response(headers={"WWW-Authenticate": "Basic"}, status_code=401)
        return response

## POST API: Update the payment status from SAP..
## Params: parcel_id
## Response: [{ OwnerID, OwnerName, Percentage }]
@app.post("/payment/sap/")
async def payment_status_SAP(auth: HTTPBasicCredentials = Depends(security), payment_upd: PaymentUpdate = Body(embed=True)):
    if not auth:
        response = Response(headers={"WWW-Authenticate": "Basic"}, status_code=401, content="Incorrect email or password")
        # return response
        return {"msg": "401 unauthorize"}

    try:
        # print(auth)
        # decoded = base64.b64decode(auth).decode("ascii")
        # username, _, password = decoded.partition(":")
        user = authenticate_user(users_db, auth.username, auth.password)
        if not user:
            raise HTTPException(status_code=400, detail="Incorrect email or password")
        parcel_id = payment_upd.Unique_Id
        [id, comp_type, land_type] = payment_upd.Text.split(";")

        paymentHistoryFC = land_table[land_type]
        paymentHistoryFields = ["drp_doc_no", "payment_doc_no", "payment_date"]

        with arcpy.da.InsertCursor(paymentHistoryFC, paymentHistoryFields) as tbList:
            for pay_detail in payment_upd.payment_detail:
                rowRecord = (payment_upd.drp_doc_no, payment_upd.payment_doc_no, payment_upd.payment_date)
                tbList.insertRow(rowRecord)
            del tbList
        return {"msg": "Payment raised succefully"}
    except:
        response = Response(headers={"WWW-Authenticate": "Basic"}, status_code=401)
        # return response
        return {"msg": "401 unauthorize"}

## SAP Integration Web App Builder
# app.mount("/", StaticFiles(directory="ui", html=True), name="ui")