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


create_land_ids()
update_required_value()