#-------------------------------------------------------------------------------
# Name:        Database Sync & Publiction Script
# Purpose:  This script is designed to pull and push feature classes and tables
#           between databases in the Testing, Staging and Production environment.
#           This script was designed to follow the 2018 GIU business model/flow.
#           At some point in the future the script will allow for different flows.
#
# Author:      John Spence, Spatial Data Administrator, City of Bellevue
#
# Created:     6/1/2018
# Modified:     6/8/2018
# Modification Purpose:
#   2018-06-08:  Added try/exceptions for FC Delete function that would typically
#               error out in the process if there was an existing FC or otherwise.
#               Added stepping stones to process to allow for layers to move to
#               multiple locations from the publication script.  User just has to
#               specify within the script that x is step 1 or not.
#               Commented out target date aquisition as the view determines the
#               time frame for updates.
#   2018-06-29:  Corrected several issues to include fixing of if final destination
#               requires projections via GISScratch, that it can end up in a
#               different instance, ie. Publishing from Test to Prod w/ projection.
#               Modified AdminGTS.Layer_Sync_Control to have an additional field
#               denoting projection of the intended target.  At present, it defaults
#               to SRID 3857, WGS 84 / Pseudo-Mercator -- Spherical Mercator,
#               Google Maps, OpenStreetMap, Bing, ArcGIS, ESRI.
#   2018-10-12:  Corrected issues w/ layers not authorized for publishing.  Script
#               now accurately shows what is going on w/ status updates on features.
#               Script also now properly publishes or not when required without
#               skipping steps in the process.
#
#   2018-10-24:  Corrected syntax error in Copy process.  Resulted from fat finger.
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# The initial configuration for the script comes from a single AD SDE connection
# that is set below for the variable db_connection.
# All other configurations are set via the publishing table contained in the
# Carta database.  See notes for dependencies below.
#
# ------------------------------- Dependencies ---------------------------------
# 1) AdminGTS.View_Find_Layers = Sorts FC vs. Tables
# 2) AdminGTS.View_Layer_Table_History = Id's Schema, Table Names, Type, and Dates
# 3) AdminGTS.View_Layers_To_Update = Meshes other tables together w/ SDE names
# 4) AdminGTS.SDE_Connections = Table that has a list of SDE connections used.
# 5) AdminGTS.Layer_Sync_Control = Table that controls where layers are sync'd to.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Configure only hard coded db connection here.
db_connection = r'Database Connections\\Connection to Carta on SQL2016STG.sde'

# Configure database update type here. (Prod, Stg, Test, Other)
db_type = 'STG'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import arcpy, datetime
from datetime import timedelta

### Setup constraints on looking back and dates
##today = datetime.date
##today = datetime.datetime.now().date()
##today = today.strftime('%m/%d/%y')
##print "The update date target is:  {0}".format(today)

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

# Def for determining what database our connection is tied to.
def set_current_database(db_connection):
    print "Entering Set Current Database----"
    global current_db
    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_db_sql)
    current_db = check_db_return
    print "Current Database:  " + current_db
    print "Leaving Set Current Database----"
    return current_db


# Def for if updates are needed check
def check_if_update (db_connection):
    print "Entering Check For Updates----"

    # Build Connection String
    data_source = db_connection + '\\ADMINGTS.View_Layers_To_Update'

    # Check record count
    global updatecnt
    updatecnt = int(arcpy.GetCount_management(data_source).getOutput(0))
    print "Records ready for update at Target-----:  {0}".format(updatecnt)
    print "Leaving Check For Updates----"
    return updatecnt

# Def for checking if layer publishes or not
def check_pub_status(db_connection, current_db, db_type, source_LayerFullName, current_step):
    print "Entering Check Publication Status----"

    # Define Global Output
    global pub_layername
    global pub_ownername
    global pub_layerfullname
    global pub_featuredataset
    global pub_sourcedb
    global pub_sourcedb_type
    global pub_scratchdb
    global pub_scratchdb_type
    global pub_targetdb
    global pub_targetdb_type
    global pub_process_step
    global pub_published
    global pub_public
    global pub_redacted
    global pub_rdMaskOwnerName
    global pub_rdMaskLayerName
    global pub_ProcessStep
    global pub_projectSRID

    # Prep variables for SQL Query
    current_db_sql = "'" + current_db + "'"
    print current_db
    db_type_sql = "'" + db_type + "'"
    print db_type
    source_LayerFullName_sql = "'" + source_LayerFullName + "'"
    print source_LayerFullName
    current_step_sql = "'{}'".format(current_step)
    print "Current Step:  {}".format(current_step)

    # Check Dbase for publication status
    check_pub_status_sql = ('''SELECT * FROM ADMINGTS.Layer_Sync_Control WHERE SourceDB = {0} and SourceDB_Type = {1} and LayerFullName = {2} and Process_Step = {3}'''
    .format(current_db_sql, db_type_sql, source_LayerFullName_sql, current_step_sql))
    check_pub_status_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_pub_status_sql)
    for row in check_pub_status_return:
        pub_layername = row[1]
        pub_ownername = row[2]
        pub_layerfullname = row[3]
        pub_featuredataset = row[4]
        pub_sourcedb = row[5]
        pub_sourcedb_type = row[6]
        pub_scratchdb = row[7]
        pub_scratchdb_type = row[8]
        pub_targetdb = row[9]
        pub_targetdb_type = row[10]
        pub_projectSRID =  row[11]
        pub_process_step = row[12]
        pub_published = row[13]
        pub_public = row[14]
        pub_redacted = row[15]
        pub_rdMaskOwnerName = row[16]
        pub_rdMaskLayerName = row[17]

        print "Leaving Check Publication Status----"

        # FIX HERE FOR NAME ISSUE
        return (pub_layername, pub_ownername, pub_layerfullname, pub_featuredataset, pub_sourcedb, pub_sourcedb_type, pub_scratchdb, pub_scratchdb_type, pub_targetdb, pub_targetdb_type, pub_projectSRID,
        pub_process_step, pub_published, pub_public, pub_redacted, pub_rdMaskOwnerName, pub_rdMaskLayerName)

def obtain_dbase_connection(db_connection, target_db, target_db_type, fc_update_owner):

    print "Entering Obtain Database Connection----"
    # Define Global Variable
    global conn_string
    print "defined global"
    # Prep variables for SQL Query
    target_db_sql = "'" + target_db + "'"
    db_type_sql = "'" + target_db_type + "'"
    fc_update_owner_sql = "'" + fc_update_owner + "'"
    print "defined variables for sQL"

    # Pull from Dbase exact connection string needed.
    db_connection_stringSQL = '''select * from admingts.SDE_Connections where SourceDB = {0} and SourceDB_Type = {1} and Data_Owner = {2}'''.format(target_db_sql, db_type_sql, fc_update_owner_sql)
    db_connection_stringReturn = arcpy.ArcSDESQLExecute(db_connection).execute(db_connection_stringSQL)
    for row in db_connection_stringReturn:
        conn_string = row [4]

        print "Database connection = {0} ".format(conn_string)
        print "Leaving Obtain Database Connection----"
        return conn_string

def check_for_existance(conn_string, target_db, item_to_check):
    print "Entering Check For Existing Layer----"

    # Configure variables for use.
    conn_string = '{0}'.format(conn_string)
    print "Using " + conn_string
    item_check = '{0}'.format(item_to_check)
    print "Checking " + item_check

    # Set current workspace environment
    arcpy.env.workspace = conn_string

    # Check for existance at target database
    if arcpy.Exists(item_check):

        print "Item exist at target."

        # Delete Existing Layer

        try:
            delete_existing_layer(conn_string, target_db, item_to_check)
            print "Leaving Check For Existing Layer----"
        except Exception as err_check_for_existance:
            print(err_check_for_existance.args[0])
            print "Leaving Check For Existing Layer----"

        return

    else:

        print "Item does not exist at target."
        print "Leaving Check For Existing Layer----"

        return

def delete_existing_layer(conn_string, target_db, item_to_check):
    print "Entering Delete Existing Layer----"

    # Configure variables for use.
    item_to_delete = '{0}\\{1}.{2}'.format(conn_string, target_db, item_to_check)

    # Delete Existing Layer
    arcpy.Delete_management (item_to_delete)

    # Print Verification of Deletion
    print item_to_check + " has been successfully deleted."

    print "Leaving Delete Existing Layer----"

    return

def copy_layer_over (input_connection, output_connection, target_db, pub_layerfullname):

    print "Entering Copy Layer----"

    # Configure Connections
    input_connection = input_connection + '\\' + current_db + '.' + pub_layerfullname
    output_connection = output_connection + '\\' + target_db + '.' + pub_layerfullname

    # Set workspace and keyword
    arcpy.env.workspace = output_connection
    arcpy.env.configKeyword= "Geometry"

    # Copy Over
    try:
        arcpy.Copy_management(input_connection, output_connection)

        print "Layer successfully copied to " + output_connection

    except Exception as err_copy_layer_over:
        print(err_copy_layer_over.args[0])

    arcpy.RecalculateFeatureClassExtent_management(output_connection)

    print "Leaving Copy Layer----"

    return

def copy_layer_over_PR (input_connection, output_connection, target_db, pub_layerfullname):
    print "Entering Copy Projection----"

    # Configure Connections
    input_connection = input_connection + '\\' + current_db + '.' + pub_layerfullname + '_PR'
    output_connection = output_connection + '\\' + target_db + '.' + pub_layerfullname

    # Set workspace and keyword
    arcpy.env.workspace = output_connection
    arcpy.env.configKeyword= "Geometry"

    # Copy Over
    try:
        arcpy.Copy_management(input_connection, output_connection)

        print "Layer successfully copied to " + output_connection
        print pub_layerfullname + " has successfully been published to its target destination of " + target_db + " on " + pub_targetdb_type + "."

    except Exception as err_copy_layer_over_PR:
        print(err_copy_layer_over_PR.args[0])

    arcpy.RecalculateFeatureClassExtent_management(output_connection)

    print "Leaving Copy Projection----"

    return

def project_layer (input_connection, output_connection, target_db, pub_layerfullname, pub_projectSRID):

    input_connection = output_connection + '\\' + target_db + '.' + pub_layerfullname
    output_connection = output_connection + '\\' + target_db + '.' + pub_layerfullname + '_PR'

    out_coordinate_system = arcpy.SpatialReference(pub_projectSRID)

    arcpy.Project_management(input_connection, output_connection, out_coordinate_system)

    ##print "Layer successfully projected to " + output_connection

    return

def step_counter (db_connection, current_db, db_type, source_LayerFullName):
    # Define Global Variable

    global step_count

    # Prep variables for SQL Query
    print "Entering Step Counter----"
    current_db_sql = "'{0}'".format(current_db)
    print current_db
    db_type_sql = "'{0}'".format(db_type)
    print db_type
    print db_type_sql
    source_LayerFullName_sql = "'{0}'".format(source_LayerFullName)
    print source_LayerFullName

    # Check Dbase for publication step count
    check_step_count_sql = """SELECT LayerFullName, COUNT({0}) FROM {1} WHERE SourceDB = {2} and SourceDB_Type = {3} and LayerFullName = {4} group by LayerFullName"""\
    .format('Process_Step', 'ADMINGTS.Layer_Sync_Control', current_db_sql, db_type_sql, source_LayerFullName_sql)
    check_step_count_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_step_count_sql)
    try:
        for row in check_step_count_return:
           step_count = row[1]
    except:
        step_count = 0
    print "Leaving Step Counter----"
    return (step_count)

#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

# Determine which database we are working in.
set_current_database(db_connection)

# Find out how many updates there are in registered database items.
check_if_update (db_connection)

#
if updatecnt > 0:
    # Pull pending updates and store as variables
    check_pending_updates_sql = '''select [Data_Owner], [Table_Name], [SDE_Name], [Type], CONVERT(VARCHAR(8), [Date_Last_Modified], 1) AS [Date_Last_Modified] from admingts.View_Layers_To_Update'''
    check_pending_updates_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_pending_updates_sql)
    for row in check_pending_updates_return:
        update_date = row[4]
        mod_date = update_date
        fc_update_owner = row[0]
        fc_update_table = row[1]
        fc_update_sde = row[2]
        fc_update_type = row [3]

        print "Current Database:  {0}".format(current_db)
        print "On Target:  {0}".format(mod_date)
        print "Update Date:  {0}".format(update_date)
        print "Schema:  {0}".format(fc_update_owner)
        print "Feature Class:  {0}".format(fc_update_table)
        print "SDE Full Name:  {0}".format(fc_update_sde)

        #Prepare Name for publication check.
        source_LayerFullName = fc_update_owner + '.' + fc_update_table
        pub_layerfullname = source_LayerFullName

        step_counter (db_connection, current_db, db_type, source_LayerFullName)
        max_loop = step_count

        current_step = 1
        print "Max Loop:  {0}".format(max_loop)
        while current_step <= max_loop:
            check_pub_status(db_connection, current_db, db_type, source_LayerFullName, current_step)

            if pub_published == 'Y':

                print "Layer authorized for publishing " + pub_layerfullname + " to " + pub_targetdb + " on " + pub_targetdb_type + "."

                if pub_scratchdb <> None:
                    # For scratch using feature class movement.
                    # Obtain database connection for Scratch.
                    target_db = pub_scratchdb
                    target_db_type = pub_scratchdb_type

                    try:
                        obtain_dbase_connection(db_connection, target_db, target_db_type, fc_update_owner)

                        #Check Target for Item
                        item_to_check = pub_layerfullname
                        check_for_existance(conn_string, target_db, item_to_check)

                        #Check Target for Projected Item
                        item_to_check = pub_layerfullname + '_PR'
                        check_for_existance(conn_string, target_db, item_to_check)

                        input_connection = db_connection
                        output_connection = conn_string

                        copy_layer_over (input_connection, output_connection, target_db, pub_layerfullname)
                        project_layer (input_connection, output_connection, target_db, pub_layerfullname, pub_projectSRID)

                        # Change target to final destination
                        current_db = target_db
                        target_db = pub_targetdb
                        target_db_type = pub_targetdb_type

                        #Change old conn_string to scratch_conn_string
                        scratch_conn_string = conn_string

                        # Pull final destination target
                        obtain_dbase_connection(db_connection, target_db, target_db_type, fc_update_owner)

                        #Check Target for Item
                        item_to_check = pub_layerfullname
                        check_for_existance(conn_string, target_db, item_to_check)

                        input_connection = scratch_conn_string
                        output_connection = conn_string

                        copy_layer_over_PR (input_connection, output_connection, target_db, pub_layerfullname)

                        set_current_database(db_connection)

                    except Exception as err:
                        print "!!!!!!!!Failure to process {0}".format(pub_layerfullname)
                        print(err.args[0])
                        set_current_database(db_connection)

                else:
                    # For non-scratch using feature class movement.
                    # Obtain database connection for Scratch.
                    target_db = pub_targetdb
                    target_db_type = pub_targetdb_type
                    obtain_dbase_connection(db_connection, target_db, target_db_type, fc_update_owner)

                    item_to_check = pub_layerfullname
                    check_for_existance(conn_string, target_db, item_to_check)

                    input_connection = db_connection
                    output_connection = conn_string

                    copy_layer_over (input_connection, output_connection, target_db, pub_layerfullname)

                    print pub_layerfullname + " has successfully been published to its target destination of " + target_db + " on " + pub_targetdb_type + "."

                current_step += 1
                if current_step > max_loop:
                    break

            else:
                print pub_layerfullname + " is not authorized to be published at this time."
                current_step += 1
                if current_step > max_loop:
                    break



else:
    print "No updates required.  Terminating connections."









