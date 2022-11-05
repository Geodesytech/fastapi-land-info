import arcpy, uuid

arcpy.env.workspace = ""

landInfo = [
 {
  "field_name": "project_id",
  "field_type": "TEXT",
  "field_length": 50,
  "field_alias": "project_id",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },{
  "field_name": "parcel_id",
  "field_type": "TEXT",
  "field_length": 50,
  "field_alias": "parcel_id",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 }
]
def create_land_ids():
    # Iterating through the json
    # list
    table_name = ""
    for f_lst in landInfo:
        arcpy.AddField_management(in_table=table_name,
                                field_name=f_lst["field_name"],
                                field_type=f_lst["field_type"],
                                field_precision=f_lst["field_precision"] if "field_precision" in f_lst else "",
                                field_scale=f_lst["field_scale"] if "field_scale" in f_lst else "",
                                field_length=f_lst["field_length"] if "field_length" in f_lst else "",
                                field_alias=f_lst["field_alias"],
                                field_is_nullable=f_lst["field_is_nullable"],
                                field_is_required=f_lst["field_is_required"])

def create_parcel_id(land_a: str, objectId: int):
    id = uuid.uuid1()
    parcel_id = "{}-{}-{}".format(land_a.lower(), str(objectId), id)
    return parcel_id

def create_project_id():
    id = uuid.uuid1()
    project_id = "proj-{}".format(id)
    return project_id

def update_required_value():
    featureClass = "ParsaLandInfo"
    fieldsList = ["OBJECTID", "land_b", "project_id", "parcel_id"]
    projectId = create_project_id()
    # Calculate field with arcpy.da.UpdateCursor
    with arcpy.da.UpdateCursor(inFeatures, field_list ) as cursor:
        for row in cursor:
            print(row[0])
            row[2] = projectId
            row[3] = create_parcel_id(row[1], row[0])
            cursor.updateRow(row)
        del cursor, row

def update_owner_info():
    parcelList = []
    landFC = "ParsaLandInfo"
    landFields = ["OBJECTID", "parcel_id", "owner_1", "owner_2", "owner_3"]
    ownerFC = "OwnerInfo"
    ownerFields = ["parcel_id", "owner_id", "owner_name", "percentage"]

    with arcpy.da.SearchCursor(landFC, landFields) as cursor:
        # parcelList = [row for row in cursor]
        for row in cursor:
            if row[2]:
                id, owner_name, percentage = str(row[2]).split(";")
                create_owner_info(row[1], owner_name, percentage)
                parcelList.append(create_owner_info(row[1], owner_name, percentage))
            if row[3]:
                id, owner_name, percentage = str(row[3]).split(";")
                create_owner_info(row[1], owner_name, percentage)
                parcelList.append(create_owner_info(row[1], owner_name, percentage))
            if row[4]:
                id, owner_name, percentage = str(row[4]).split(";")
                create_owner_info(row[1], owner_name, percentage)
                parcelList.append(create_owner_info(row[1], owner_name, percentage))

        del cursor

    print(parcelList)

    with arcpy.da.InsertCursor(ownerFC, ownerFields) as tbList:
        for row in parcelList:
            tbList.insertRow(row)
        del tbList


SAPInfo = [
 {
  "field_name": "project_id",
  "field_type": "TEXT",
  "field_length": 50,
  "field_alias": "project_id",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "check_list",
  "field_type": "SHORT",
  "field_precision": 2,
  "field_length": 2,
  "field_alias": "check_list",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "fiscal_year",
  "field_type": "SHORT",
  "field_precision": 4,
  "field_length": 4,
  "field_alias": "fiscal_year",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "company_code",
  "field_type": "TEXT",
  "field_length": 4,
  "field_alias": "company_code",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "vendor",
  "field_type": "LONG",
  "field_precision": 16,
  "field_scale": 0,
  "field_length": 16,
  "field_alias": "vendor",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "sp_g_l",
  "field_type": "TEXT",
  "field_length": 4,
  "field_alias": "sp_g_l",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "withholding_tax_type",
  "field_type": "TEXT",
  "field_length": 15,
  "field_alias": "withholding_tax_type",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "witholding_tax_code",
  "field_type": "TEXT",
  "field_length": 15,
  "field_alias": "witholding_tax_code",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "tax_code",
  "field_type": "TEXT",
  "field_length": 2,
  "field_alias": "tax_code",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "business_place",
  "field_type": "TEXT",
  "field_length": 4,
  "field_alias": "business_place",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "section_code",
  "field_type": "TEXT",
  "field_length": 4,
  "field_alias": "section_code",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "business_area",
  "field_type": "TEXT",
  "field_length": 4,
  "field_alias": "business_area",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "payment_ref",
  "field_type": "TEXT",
  "field_length": 23,
  "field_alias": "payment_ref",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "profit_center",
  "field_type": "TEXT",
  "field_length": 10,
  "field_alias": "profit_center",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "assignment",
  "field_type": "TEXT",
  "field_length": 18,
  "field_alias": "assignment",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "text",
  "field_type": "TEXT",
  "field_length": 50,
  "field_alias": "text",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "bank_partner_type",
  "field_type": "TEXT",
  "field_length": 4,
  "field_alias": "bank_partner_type",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "doc_sub_category",
  "field_type": "SHORT",
  "field_precision": 6,
  "field_scale": 0,
  "field_length": 6,
  "field_alias": "doc_sub_category",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "remark",
  "field_type": "TEXT",
  "field_alias": "remark",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 },
 {
  "field_name": "land_act",
  "field_type": "TEXT",
  "field_alias": "land_act",
  "field_is_nullable": "NON_NULLABLE",
  "field_is_required": "REQUIRED"
 }
]
output_path= r"C:\Stuff\DR-AKSHAY\SAP-Integration\DB\GDB"
gdb_filename = "land_info.gdb"
gdb_path = "{}\\{}".format(output_path, gdb_filename)
arcpy.env.workspace = gdb_path

sapFC = "SAPInfo"

def create_sap_info():
    # Iterating through the json
    # list
    for f_lst in SAPInfo:
        arcpy.AddField_management(in_table=sapFC,
                                field_name=f_lst["field_name"],
                                field_type=f_lst["field_type"],
                                field_precision=f_lst["field_precision"] if "field_precision" in f_lst else "",
                                field_scale=f_lst["field_scale"] if "field_scale" in f_lst else "",
                                field_length=f_lst["field_length"] if "field_length" in f_lst else "",
                                field_alias=f_lst["field_alias"],
                                field_is_nullable=f_lst["field_is_nullable"],
                                field_is_required=f_lst["field_is_required"])

create_sap_info()