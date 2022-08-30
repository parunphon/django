import os
import sys
import time
import ntpath
import datetime
import traceback
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By

AutoAssignDir='auto_assign'

def get_data_fleetcap(active_dir,output_name='FleetCap'):
    import gspread    
    from oauth2client.service_account import ServiceAccountCredentials
    from datetime import timedelta, date

    print ("active_dir",active_dir)

    ######################## Set Date Time and Template File Name ################################
    D1 = date.today() - timedelta(days=0)
    M1 = D1.strftime("%Y_%m%b")
    D1 = D1.strftime("%d-%m-%Y")

    file_name = active_dir+r'\\'+output_name+'_Template_'+M1+'.xlsx'

    print(file_name)
      
    ########## Read Transporter Code and Shipping Type from Template #########################
    google_sheet = 'Sum_Fleet_Cap'
    ########################## Get Data From Google Sheet ####################################
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    # Your json file here
    credentials = ServiceAccountCredentials.from_json_keyfile_name('zinc-proton-268015-d2cf843be317.json', scope)
    gc = gspread.authorize(credentials)
    i = 0
    while True:
        try:
            wks = gc.open(google_sheet).sheet1
            data = wks.get_all_values()
            print("Found Google Sheet :", google_sheet)
            status = True
        except:
            time.sleep(5)
            print("Not Found", google_sheet)
            i = i + 1
            if i >= 20:
                fleetcap.append(0)
                status = False
                break
            continue
        break

    ########################### Get Fleet Cap Data ##########################################
    if status == True :      
        headers = data.pop(0)
        df_gd = pd.DataFrame(data, columns=headers)
        
        cols = ['Shipping Type','Sub Shipping Type','Transporter Code', 'Transporter name', 'Fleet commit', 'Daily fleet cap','Net Weight Qty','Shift']
        df_pm = df_gd[cols]    
        df_pm = df_pm.rename(columns = {'Daily fleet cap':'FleetCap'})
        df_pm.loc[:,'FleetCap'] = df_pm['FleetCap'].replace(' ','0',regex=True)
        df_pm.loc[:,'FleetCap'] = df_pm['FleetCap'].replace('','0',regex=True)
        df_pm.loc[:,'FleetCap'] = df_pm['FleetCap'].replace('-','0',regex=True)
        df_pm.loc[:,'FleetCap'] = df_pm['FleetCap'].replace('#REF!','0',regex=True)
        df_pm['FleetCap'] = df_pm['FleetCap'].fillna(0)
        
        df_pm['FleetCap']= df_pm['FleetCap'].astype(int)
    output_name='FleetCap'
    sheet_df = df_pm
    sheet_df = sheet_df.fillna('0')
    writer = pd.ExcelWriter(file_name)
    sheet_df.to_excel(writer, output_name, index=False)
    print("Write Fleet Cap already") 
    writer.save()
    writer.close()
    
    #get_data_fleetcap_history(active_dir,sheet_df,output_name='FleetCap')
    
    return sheet_df

def get_data_fleetcap_history(active_dir,dataframe,output_name='FleetCap'):
    from datetime import timedelta, date
    df = dataframe
    ######################## Set Date Time and Template File Name ################################          

    df.insert(8, 'TimeStamp', pd.datetime.now().replace(microsecond=0))
    ############################# Get Exiting Excel Template ##################################
    # load existing data
    file_name = active_dir+'\\FleetCap_History_Template.xls'
    output_name='FleetCap'

    if os.path.exists(file_name):
        print("Found Template File") 
        df2 = pd.read_excel(file_name,sheet_name = output_name)
        df2 = pd.DataFrame(df2)
        print(df2)
        sheet_df = df.append(df2, sort=False)
    else:
        print("Not Found Template File")     
        sheet_df = df
     
    #print(sheet_df)
    writer = pd.ExcelWriter(file_name)
    sheet_df.to_excel(writer, output_name, index=False)
    print("Write Fleet Cap History already") 
    writer.save()
    writer.close()   
        
    return sheet_df
  
def get_data_bhl_index_tzone(tzone):    
    import configparser
    # Import data from Config File 
    config = configparser.ConfigParser()
    configFilePath = os.path.dirname(os.path.realpath(__file__))+'\config.ini'    
    config.read(configFilePath)
    
    if config.has_option("LCC-bhl-index-tzone",tzone) == True: 
        bhl_index = config.get('LCC-bhl-index-tzone',tzone)
    else:
        bhl_index = "NoBHL"
    return bhl_index

def get_data_bhl_index_transporter(transporter):    
    import configparser
    # Import data from Config File 
    config = configparser.ConfigParser()
    configFilePath = os.path.dirname(os.path.realpath(__file__))+'\config.ini'    
    config.read(configFilePath)
    
    if config.has_option("LCC-bhl-index-transporter",transporter) == True: 
        bhl_index = config.get('LCC-bhl-index-transporter',transporter)
    else:
        bhl_index = "NoBHL"
    return bhl_index

def otm_login_sql(username,password):
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    
    # Set Chrome Driver Path
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())
    driver.implicitly_wait(5)
    driver.maximize_window()
    
    # OTM LogIn Page 
    driver.get('https://otmgtm-a522235.otm.em2.oraclecloud.com/GC3/glog.webserver.sql.SqlServlet')
    driver.find_element_by_id("username").send_keys(username)
    driver.find_element_by_id("password").send_keys(password)
    driver.find_element_by_id("signin").click()
    
    return driver

def otm_login_prd(username,password):
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    
    # Set Chrome Driver Path
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install())
    driver.implicitly_wait(5)
    driver.maximize_window()
    
    # OTM LogIn Page 
    driver.get("https://otmgtm-a522235.otm.em2.oraclecloud.com")
    driver.find_element_by_id("username").send_keys(username)
    driver.find_element_by_id("password").send_keys(password)
    driver.find_element_by_id("signin").click()
    
    return driver
   
def otm_change_role(rolename,driver):
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    
    wait = WebDriverWait(driver,20)
    element = wait.until(EC.element_to_be_clickable((By.XPATH,"//canvas[@id='userNameCanvas']"))) 
    element.click()

    time.sleep(2)
    driver.find_element_by_id("r1:0:dl3::content").click()
    dropdown = driver.find_element_by_id("r1:0:dl3::content")
    dropdown.find_element_by_xpath("//option[. = '"+ rolename +"']").click()
    driver.find_element_by_id("r1:0:dl3::content").click()
    driver.find_element_by_link_text("Save and Close").click()

def get_download_filename(active_dir):
    before=after = os.listdir(active_dir)
    file_name='crdownload'
    while set(after) == set(before) or 'crdownload' in file_name:
        time.sleep(3)
        after = os.listdir(active_dir)
        change = set(after) - set(before)
        if len(change) == 1:
            file_name = change.pop()
        print(file_name)
    return file_name

def set_download_tempdir(activedir):    
    D1 = r'\\'+datetime.datetime.now().strftime("temp_%Y%m%d%H%M%S")
    tempdir=activedir  + D1
    if not os.path.exists(tempdir): os.makedirs(tempdir)        
    return tempdir

def otm_download(sql,var=None,chromePath=None): #sql : sql script or path+file
    import os
    from selenium.webdriver.common.keys import Keys
    download_option=1
    #tempdir= set_download_tempdir(active_dir) if download_option==1  else active_dir
    active_dir = os.environ['USERPROFILE']+ r'\downloads\\'
    driver = otm_login_sql('sccc-otm-bot1','Pass12345')
    
    driver.switch_to.frame(0)
    driver.find_element_by_id("toSpreadsheet").click()
    driver.find_element_by_name("count@PRF").clear()
    driver.find_element_by_name("count@PRF").send_keys("99999")
 
    if '.sql' in sql and len(sql)<250:
        sql=otm_query(sql,var)
    else:
        import xerox #pip install xerox
        xerox.copy(sql)        
        os.system("echo %s| clip" % sql.strip())
        
    driver.find_element_by_id("sql").send_keys(sql)
    #before=os.listdir(active_dir)
    driver.find_element_by_xpath("//button[@class='enButton']").click()
    file_name = get_download_filename(active_dir)
    
    #driver.close()
    return (file_name)

def otm_bulkplan_group(cpdo,driver):
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    
    Loop = 0
    try:
        ##### Start Program #####
        driver.refresh()
        time.sleep(1)

        for handle in driver.window_handles:
            driver.switch_to.window(handle)
        time.sleep(3)
        
        try:
            driver.switch_to.frame(1)
        except:
            driver.find_element_by_link_text("Unplanned").click()
            driver.switch_to.frame(1)

        driver.find_element(By.NAME, "order_release/xid").send_keys(cpdo)
         
        #Search Shipment by CPDO number
        dropdown = driver.find_element(By.NAME, "order_release/xid_operator")
        if "," in cpdo: 
            dropdown.find_element(By.XPATH, "//option[. = 'One Of']").click()
        else:
            dropdown.find_element(By.XPATH, "//option[. = 'Begins With']").click()
        
        driver.find_element(By.ID, "search_button").click()
        
        #Check Missing CPDO  
        try:
            element=driver.find_element_by_id("rgNoDataMsg")
            print ("No Data Found")
            status = ""
            driver.refresh()
          
        except:
            print ("Data Found")

            wait = WebDriverWait(driver,30)
            element = wait.until(EC.element_to_be_clickable((By.XPATH,'//span[contains(text(),"Records")]')))
            time.sleep(1)#99999
            driver.find_element(By.ID, 'rgSGSec.1.1.1.1.check').click()
            
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(2)
            driver.switch_to.frame(1)
            
            # Click Side Menu Tree
            time.sleep(2)
            driver.find_element(By.CSS_SELECTOR, ".finderActionButtonImg").click()
            wait = WebDriverWait(driver,20)
            element = wait.until(EC.frame_to_be_available_and_switch_to_it("actionFrame"))
            time.sleep(2)
            driver.find_element(By.ID, "actionTree.1_3.k").click()
            
            time.sleep(2)  
            driver.switch_to.window(driver.window_handles[1])#99999
            time.sleep(1) #<<< Adjust 22Dec2020
            driver.maximize_window()
                
            time.sleep(3)#<<< Adjust 09/03/21
            driver.switch_to.frame(1)
            time.sleep(3) 
            driver.find_element(By.ID, "ok").click()
            
            # Refresh and Waiting Bulk Plan Result
            time.sleep(3)#99999
            elem1 = elem2 = elem3 = "0"
            while True:
                driver.find_element(By.NAME,"refreshButton").click()
                time.sleep(2)#99999 
                for elem1 in driver.find_elements_by_xpath('//tr[1]//td[4]//div[1]//div[2]'):
                    elem1 = elem1.text
                for elem2 in driver.find_elements_by_xpath("//div[@id='resultsData']//tr[2]//td[3]//div[1]//div[2]"):
                    elem2 = elem2.text
                if(elem1 != "0" or elem2 != "0" or Loop > 5 ):    
                    break
                Loop = Loop + 1
         
            # Assign Bulk Plan Status
            status = ""    
            if int(elem1) > 0:
                status = "Bulk Plan Pass"
                # Add Shipment Number
                time.sleep(1)#99999
                try:
                    driver.find_element(By.LINK_TEXT, "1").click()
                except:
                    driver.find_element(By.LINK_TEXT, "2").click()
                
                for handle in driver.window_handles:
                    driver.switch_to.window(handle)
                time.sleep(3)
                driver.switch_to.frame(1)
                for elem3 in driver.find_elements_by_xpath('//*[@id="rgSGSec.2.2.1.1"]'):
                    elem3 = elem3.text         
                print("Shipment Number = ",elem3)
                status = status + " : " + elem3       
                driver.close()
                
            else:
                status = "<font color='red'> Bulk Plan Fail </font>"
                
            # Close Bulk Plan Pop-up Windows
            for handle in driver.window_handles:
                driver.switch_to.window(handle)

            driver.switch_to.frame(1)
            driver.close()

            # Back to Main Windows and Clear all CPDO Data
            driver.switch_to.window(driver.window_handles[0])
            driver.refresh()
    except:
        status = "<font color='red'> Process Fail @ Bulk Plan </font>"
        # Close Bulk Plan Pop-up Windows
        for handle in driver.window_handles:
            driver.switch_to.window(handle)
        driver.switch_to.frame(1)
        driver.close()

    return (status)

def otm_bulkplan_group_backup(cpdo,driver):
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    
    Loop = 0
    try:
        ##### Start Program #####
        driver.refresh()
        time.sleep(1)

        for handle in driver.window_handles:
            driver.switch_to.window(handle)
        time.sleep(3)
        
        try:
            driver.switch_to.frame(1)
        except:
            driver.find_element_by_link_text("Unplanned").click()
            driver.switch_to.frame(1)

        driver.find_element(By.NAME, "order_release/xid").send_keys(cpdo)
         
        #Search Shipment by CPDO number
        dropdown = driver.find_element(By.NAME, "order_release/xid_operator")
        if "," in cpdo: 
            dropdown.find_element(By.XPATH, "//option[. = 'One Of']").click()
        else:
            dropdown.find_element(By.XPATH, "//option[. = 'Begins With']").click()
        
        driver.find_element(By.ID, "search_button").click()
        
        #Check Missing CPDO  
        try:
            element=driver.find_element_by_id("rgNoDataMsg")
            print ("No Data Found")
            status = ""
            driver.refresh()
          
        except:
            print ("Data Found")

            wait = WebDriverWait(driver,30)
            element = wait.until(EC.element_to_be_clickable((By.XPATH,'//span[contains(text(),"Records")]')))
            time.sleep(1)#99999
            driver.find_element(By.ID, 'rgSGSec.1.1.1.1.check').click()
            
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(2)
            driver.switch_to.frame(1)
            
            # Click Side Menu Tree
            time.sleep(2)
            driver.find_element(By.CSS_SELECTOR, ".finderActionButtonImg").click()
            wait = WebDriverWait(driver,20)
            element = wait.until(EC.frame_to_be_available_and_switch_to_it("actionFrame"))
            time.sleep(2)
            driver.find_element(By.ID, "actionTree.1_3.k").click()
            
            time.sleep(2)  
            driver.switch_to.window(driver.window_handles[1])#99999
            time.sleep(1) #<<< Adjust 22Dec2020
            driver.maximize_window()
                
            time.sleep(2)
            driver.switch_to.frame(1)
            time.sleep(3) #<<< Adjust 22Dec2020
            driver.find_element(By.ID, "ok").click()
            
            # Refresh and Waiting Bulk Plan Result
            time.sleep(3)#99999
            elem1 = elem2 = elem3 = "0"
            while True:
                driver.find_element(By.NAME,"refreshButton").click()
                time.sleep(2)#99999 
                for elem1 in driver.find_elements_by_xpath('//tr[1]//td[4]//div[1]//div[2]'):
                    elem1 = elem1.text
                for elem2 in driver.find_elements_by_xpath("//div[@id='resultsData']//tr[2]//td[3]//div[1]//div[2]"):
                    elem2 = elem2.text
                if(elem1 != "0" or elem2 != "0" or Loop > 5 ):    
                    break
                Loop = Loop + 1
         
            # Assign Bulk Plan Status
            status = ""    
            if int(elem1) > 0:
                status = "Bulk Plan Pass"
                # Add Shipment Number
                time.sleep(1)#99999
                try:
                    driver.find_element(By.LINK_TEXT, "1").click()
                except:
                    driver.find_element(By.LINK_TEXT, "2").click()
                
                for handle in driver.window_handles:
                    driver.switch_to.window(handle)
                time.sleep(3)
                driver.switch_to.frame(1)
                for elem3 in driver.find_elements_by_xpath('//*[@id="rgSGSec.2.2.1.1"]'):
                    elem3 = elem3.text         
                print("Shipment Number = ",elem3)
                status = status + " : " + elem3       
                driver.close()
                
            else:
                status = "<font color='red'> Bulk Plan Fail </font>"
                
            # Close Bulk Plan Pop-up Windows
            for handle in driver.window_handles:
                driver.switch_to.window(handle)

            driver.switch_to.frame(1)
            driver.close()

            # Back to Main Windows and Clear all CPDO Data
            driver.switch_to.window(driver.window_handles[0])
            driver.refresh()
    
    except:
        status = "<font color='red'> Process Fail @ Bulk Plan </font>"
        # Close Bulk Plan Pop-up Windows
        for handle in driver.window_handles:
            driver.switch_to.window(handle)
        driver.switch_to.frame(1)
        driver.close()  

    return (status)

####################### Bulk Plan for  Merge CPDO  ######################
#Loop for set BHL Index to Shipment
def set_merge_qual_dbp(merge_group,merge_bhl,shCount,shNumber,merge_tran,driver,merge_req):
    
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        
    print("Count of Key",len(merge_group.keys()))
    for key in merge_group:
        try: #99999 @ 15Oct2020
            time.sleep(1)
            #Load Page and Click Tab Order Release
            print('Bulk Plan Merge ->',merge_group[key])
            status = otm_bulkplan_group(merge_group[key],driver)
            if status != "":
                shCount = shCount + 1
                print("Status Found",status)
                if merge_bhl[key] != 'nan': 
                    shNumber = shNumber + merge_group[key] + " " +  merge_req[key] + ": " + merge_tran[key] + " : " + status + " : " + merge_bhl[key] + "<br>"
                else:
                    shNumber = shNumber + merge_group[key] + " " +  merge_req[key] + ": " + merge_tran[key] + " : " + status + "<br>"
        except: #99999 @ 15Oct2020
            shCount = shCount + 1
            shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : <font color='red'> Process Fail @ otm_bulkplan_group </font> <br>"
            driver.refresh()
            
    return shCount,shNumber

####################### Bulk Plan for  Merge CPDO  ######################
#Loop for set BHL Index to Shipment
def set_merge_qual_uat(merge_group,merge_bhl,shCount,shNumber,merge_tran,driver):
    
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        
    print("Count of Key",len(merge_group.keys()))
    for key in merge_group:
        time.sleep(1)
        #Load Page and Click Tab Order Release
        print('Bulk Plan Merge ->',merge_group[key])
        status = otm_bulkplan_group(merge_group[key],driver)
        if status != "":
            shCount = shCount + 1
            print("Status Found",status)
            if merge_bhl[key] != 'nan': 
                shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : " + status + " : " + merge_bhl[key] + "<br>"
            else:
                shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : " + status + "<br>"
        #driver.refresh() #99999 @ 29/10/2020
         
    return shCount,shNumber

# Set Merge Group and Bulk Plan
def set_merge_qual(merge_group,shCount,shNumber,merge_tran,driver):   
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        
    print("Count of Key",len(merge_group.keys()))
    for key in merge_group:
        try: 
            time.sleep(1)
            #Load Page and Click Tab Order Release
            print('Bulk Plan Merge ->',merge_group[key])
            status = otm_bulkplan_group(merge_group[key],driver)
            shCount = shCount + 1
            shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : " + status + "<br>" 

        except:
            shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : <font color='red'> Process Fail </font> <br>"
            driver.refresh()
            continue
    return shCount,shNumber

# Check Weight for 6W before Bulk Plan
def set_merge_qual_6W(merge_group,shCount,shNumber,merge_tran,merge_weight,minweight,merge_text,merge_bhl,driver):
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        
    print("Count of Key",len(merge_group.keys()))
     
    for key in merge_group:
        try: #99999 @ 30Sep2020
            if float(merge_weight[key]) < float(minweight):
                if "เต็มเที่ยว" in str(merge_text[key]) or "เหมา" in str(merge_text[key]) :
                    print('Found Text Note : for Min Weight Allow')
                    status = otm_bulkplan_group(merge_group[key],driver)
                else:
                    status = "<font color='red'> Weight Fail </font>" 

                if status != "" and merge_bhl[key] != 'nan' :
                    shNumber = shNumber +  merge_group[key] + " : " + merge_tran[key] + " : " + status + " : " + merge_bhl[key] + "<br>"   
                    shCount = shCount + 1
                elif status != "" :
                    shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : " + status + "<br>"
                    shCount = shCount + 1
            else: 
                time.sleep(1)
                #Load Page and Click Tab Order Release
                print('Bulk Plan Merge ->',merge_group[key])
                status = otm_bulkplan_group(merge_group[key],driver)
                print("status >>>>> ",status)
                if status != "":
                    shCount = shCount + 1
                    if merge_bhl[key] != 'nan' :
                        shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : " + status + " : " + merge_bhl[key] + "<br>" 
                    else:
                        shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : " + status + "<br>"
        except:
            shCount = shCount + 1
            shNumber = shNumber + merge_group[key] + " : " + merge_tran[key] + " : <font color='red'> Process Fail @ otm_bulkplan_group </font> <br>"
            driver.refresh()

    return shCount,shNumber

def set_bhl_index(bhl_group,driver):
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
    
    print("Count of Key",len(bhl_group.keys()))
    for key in bhl_group:
        try:
            #Goto Home Menu > Shipment > Planned
            driver.find_element(By.ID, 'homecanvas').click()
            time.sleep(2)
            driver.find_element_by_link_text("Shipments").click()
            time.sleep(5)
            
            try:
                driver.find_element_by_link_text("Planned").click()
            except:
                driver.find_element_by_link_text("Shipments").click()
                time.sleep(1)
                driver.find_element_by_link_text("Planned").click()
                
            #Load Page and Click Tab Order Release
            time.sleep(2)#9999
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(5)#9999
            driver.switch_to.frame(1)
            driver.find_element(By.LINK_TEXT, "Order Release").click()
            
            #Search Shipment in BHL Group by CPDO
            print (key, 'bhl_group Key -> ', bhl_group[key])
            time.sleep(2)
            driver.find_element(By.ID, "shipment/order_rel_xid").send_keys(bhl_group[key])
            time.sleep(3)
            driver.find_element(By.NAME, "shipment/order_rel_xid_operator").click()
            dropdown = driver.find_element(By.NAME, "shipment/order_rel_xid_operator")
            
            if "," in bhl_group[key]:
                dropdown.find_element(By.XPATH, "//option[. = 'One Of']").click()
            else:
                dropdown.find_element(By.XPATH, "//option[. = 'Begins With']").click()
            
            driver.find_element(By.NAME, "search_button").click()
            time.sleep(5)
            
            #Check Shipment Before Assign BHL Index
            try:
                element=driver.find_element_by_id("rgNoDataMsg")
                print ("No Data Found")
                #continue
            except:
                time.sleep(1)
                print ("Data Found")
                
                #Click All Check Box
                driver.find_element(By.ID, 'rgSGSec.1.1.1.1.check').click()
                
                #Click Mass Update Icon
                time.sleep(3)    
                driver.find_element(By.ID, 'rgMassUpdateImg').click()
                
                #Fill in Transporter Code and Sub ShppingType
                time.sleep(2)
                driver.switch_to.frame(2)
                time.sleep(3)
                driver.find_element(By.ID, 'shipment/attribute9').send_keys(key)

                # Click OK and Pop-Up windows
                time.sleep(1)
                driver.switch_to.window(driver.window_handles[0])
                driver.switch_to.frame(1)
                time.sleep(1)
                driver.find_element(By.XPATH, "//button[@id='resultsPage:MassUpdatePopupDialog::save']").click()
                time.sleep(15)
                driver.find_element(By.ID, "saveClose").click()
            driver.refresh()
            
        except Exception as ex:
            error_msg = traceback.format_exc()
            error = '<br><font color="red">'+ error_msg + "</font><br>"        
            send_mail_error(os.path.basename(__file__),error)
   
def send_mail_auto_assign(subject,body_main,shCount,sh_detail_html,attached_file='No'):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    #### Connect Gmail Server by Port SMTP ####
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.ehlo()
    server.starttls() #and this method to begin encryption of messages

    #### Change Usernam and Password ####
    server.login("parunphon.lonapalawong@siamcitycement.com", "pvclkdpsdtvnnvcq")
    
    To = 'sccc-logisticscontrolcenter@siamcitycement.com'
    #To = 'parunphon.lonapalawong@siamcitycement.com'

    Cc = ('SCCC-OTM-admin@siamcitycement.com,UCKAROOT.KAJOHNSALINGKARN@SIAMCITYCEMENT.COM,'+
          'Tanakron.Mankongchokdee@siamcitycement.com,chayut.bhongdhani@siamcitycement.com,'+
          'VIRITPON.JAROENWIPHATSIRI@SIAMCITYCEMENT.COM,SURAPAN.HERABAT@SIAMCITYCEMENT.COM,'+
          'Louis.Wangmanaopitak@siamcitycement.com,APANUCH.SRANGSRIWONG@SIAMCITYCEMENT.COM')

    #Cc = ('parunphon.lonapalawong@siamcitycement.com')

    ##### Email subject, from , to will be defined here #####
    msg = MIMEMultipart()
    msg['From'] = "sccc-otm-admin@siamcitycement.com"
    msg['To'] = To
    msg['Cc'] = Cc

    msg['Subject'] = subject

    msg_html = """\

    <html>
      <head></head>
      <body>
      <body><font face="Microsoft Sans Serif">
        <p>เรียน ทีม LCC<br>
            <br>ระบบอัตโนมัติทำการจัดงาน """+ body_main +""" มีการส่งงานไปยังผู้ขนส่งเรียบร้อยแล้วสำหรับรายการตามด้านล่าง
            <br>ทั้งนี้ หากพบข้อผิดพลาดประการใด รบกวนตอบกลับมาในอีเมล์ SCCC-OTM-Admin@siamcitycement.com ค่ะ<br>
            <br><u>จัดงานอัตโนมัติจำนวน """+ str(shCount) +  """  รายการตามด้านล่าง</u>
            <br>""" + sh_detail_html +  """<br>
            <br>เรียนมาเพื่อทราบและขอแสดงความนับถือ<br>
            <br><font color="red">**หมายเหตุ อีเมลนี้เป็นอีเมลอัตโนมัติ โปรดอย่าตอบกลับอีเมลฉบับนี้**</font><br>
            <br>Logistics Excellent Robotic<br>
        </p></font>
      </body>
    </html>
    """ 
    #### Send Mail > Move from Below ######
    if attached_file != 'No':
        from email.mime.base import MIMEBase
        from email import encoders
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(attached_file, "rb").read())
        encoders.encode_base64(part)
        
        File_Name = attached_file.rsplit("\\",1)[1]
        part.add_header('Content-Disposition', 'attachment; filename='+File_Name)
        msg.attach(part)

    ##### Add message to Mail and Send ####
    msg.attach(MIMEText(msg_html, 'html'))
    server.sendmail(msg["From"], msg["To"].split(",") + msg["Cc"].split(","), msg.as_string())
    server.close()  

    print("Send Mail Already") 

def send_mail_with_attached(to,cc,subject,attached_file,html_body):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    from jinja2 import Environment        # Jinja2 templating
    
    #server = smtplib.SMTP('smtp.office365.com', 587)
    server = smtplib.SMTP('10.254.1.244', 25)
    server.ehlo()
    #server.starttls() #and this method to begin encryption of messages
    #server.login()
    #server.login("parunphon.lonapalawong@siamcitycement.com", "pvclkdpsdtvnnvcq")

    # email subject, from , to will be defined here
    msg = MIMEMultipart()
    msg['From'] = "sccc-otm-admin@siamcitycement.com"
    msg['To'] = to
    msg['Cc'] = cc
    msg['Subject'] = subject
    
    html = """\
    <html>
      <head></head>
      <body>
      <body><font face="Microsoft Sans Serif">
        <p>Dear All<br>
            <br><u>OTM Report : """+ subject + """</u><br>
            <br>"""+ html_body +"""<br>
            <br>Best Regards,
            <br>Logistics Excellent Robotic<br>
        </p></font>
      </body>
    </html>
    """           

    msg.attach(MIMEText(html, 'html'))
    
    if attached_file != "":
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(attached_file, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename='+attached_file.rsplit("\\",1)[1])
        msg.attach(part)

    server.sendmail(msg["From"], msg["To"].split(",") + msg["Cc"].split(","), msg.as_string())
    print("Send Mail Already")
    server.close()   

def send_mail_error(program,error):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls() #and this method to begin encryption of messages
    server.login("sccc2bot4otm@gmail.com", "Bot15965")

    # email subject, from , to will be defined here
    msg = MIMEMultipart()
    msg['From'] = "sccc2bot4otm@gmail.com"
    #msg['To'] = 'parunphon.lonapalawong@siamcitycement.com'
    msg['To'] = 'parunphon.lonapalawong@siamcitycement.com,PANITSORN.RODWINIT@SIAMCITYCEMENT.COM,ORAPARN.MEECHAKA@SIAMCITYCEMENT.COM'
    msg['Subject'] = 'OTM found Error in Program : '+ program

    error_html = """\
    <html>
      <head></head>
      <body>
      <body><font face="Microsoft Sans Serif">
        <p>Dear OTM-Admin-Group<br>
            <br><u>OTM BOT Found Error in Program as below lists : </u><br>
            <br><font color="red">"""+error+"""</font> 
            <br>Logistics Excellent Robotic<br>
        </p></font>
      </body>
    </html>
    """       
    msg.attach(MIMEText(error_html, 'html'))

    server.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
    print("Send Error Mail Already")
    server.close()   
    
def read_textnote(result_dir,df,master_filter=''): # Text File Mapping for 'BAG_18W_NE'
    import TextRead_v01 as tr
    import time
    
    start = time.time()
    # change / * to -
    df.loc[:,'Delivery_note'] = df['Delivery_note'].replace('/','-',regex=True)
    df.loc[:,'Delivery_note'] = df['Delivery_note'].str.replace('*','-',regex=True)
    df.loc[:,'Delivery_note'] = df['Delivery_note'].replace("[Req. Date]",'',regex=True)
    df.loc[:,'Delivery_note'] = df['Delivery_note'].replace('[REQ. DATE]','',regex=True)
    df.loc[:,'Delivery_note'] = df['Delivery_note'].replace('-2019',':',regex=True)
    df.loc[:,'Delivery_note'] = df['Delivery_note'].replace('-2020',':',regex=True)
    df.loc[:,'Delivery_note'] = df['Delivery_note'].str.replace('|','',regex=True)
    df_dic = pd.read_excel(result_dir+r'\MasterDict_Template.xlsx',index = False)
    if master_filter != '' :
        df_dic = df_dic[(df_dic['Shipping Type'] == master_filter) &(df_dic['Value'].notnull())]
    
    for index, row in df.iterrows():
        if str(row['Assign Date'])=="NaT" and len(row['Delivery_note']) > 8:
            TextDeliveryNote = str(row['Delivery_note'])[8:len(str(row['Delivery_note']))]
            DeliveryNote = tr.DeliveryNote(TextDeliveryNote,df_dic) 
            #DeliveryNote = tr.DeliveryNote(str(row['Delivery_note']),df_dic)
            print("Delivery Note",index,":", row['Delivery_note'], "=", DeliveryNote)
            df.loc[index, 'TransCode_textNote'] = DeliveryNote
        else:
            df.loc[index, 'TransCode_textNote'] = None
            #df.loc[index, 'TransCode_textNote'] = ""           
            
    df1 = df[['PreDONo','TransCode_textNote']]   
    end = time.time()
    print('read_textnote',len(df[df['Delivery_note'].notnull()]),' PreDos : Time to execute(sec.):',(end-start))
    return df['TransCode_textNote']

def TzoneMatched(master,df_allday):
    ##### P' Keng / Nuttapong Functions
    import collections
    # Read master file
    dfMaster = pd.read_excel(master,index = False, sheet_name = '6WData')
    dfMaster['FirstLeg'] = dfMaster['FirstLeg'].astype(str)
    dfMaster['SecondLeg'] = dfMaster['SecondLeg'].astype(str)
    
    # Read Tzone file
    df = df_allday
    df = df.fillna('0')
    
    df.Transporter = df.Transporter.astype(str)
    df['Tzone'] = df['Tzone'].astype(str)
    df['Tzone2'] = df['Tzone2'].astype(str)
    
    print(df)
    # create list of Tzone
    ls = df['Tzone'].tolist()
    ls2 = df['Tzone2'].tolist()

    dfList = []

    for counter,value in enumerate(ls):
        df1 = df.copy()
        df1.loc[:,'Tzone1'] = value
        df1.loc[:,'Tzone2'] = df1['Tzone2'].drop(counter,axis =0)
        dfList.append(df1)

    dfFinal = pd.concat(dfList)
    dfFinal.drop(['Tzone'],axis = 1,inplace = True)
    dfFinal.dropna(inplace=True)
    
    # combine
    dfFinal = dfFinal.merge(dfMaster,how='left',left_on = ['Tzone1','Tzone2'],right_on = ['FirstLeg','SecondLeg'])
    dfFinal[['Frequency','Possibility']] = dfFinal[['Frequency','Possibility']].fillna(0)
    dfFinal = dfFinal[['FirstLeg','SecondLeg','Possibility']]

    dfFinal.drop_duplicates(inplace = True)
    dfFinal.sort_values(by = 'Possibility',ascending = False, inplace = True)
    dfFinal.reset_index(drop = True,inplace = True)
    
    print(dfFinal)
    
    # create matching table
    counterOri=collections.Counter(ls)
    counter=collections.Counter(ls2)
    print("List of Tzone Origin: ",counter)

    ls1, ls2 = [],[]

    for i in ls:
        if counter[i] > 0:
            counter[i] = counter[i]-1
            try:
                rowPossible = len(dfFinal[dfFinal['FirstLeg'] == i])
                
                if rowPossible ==0:
                    lst1.append(i)
                    ls2.append("")
                    break

                for j in range(rowPossible):
                    secLeg = dfFinal[dfFinal['FirstLeg'] == i]['SecondLeg'].iloc[j]
                    if counter[secLeg] > 0:
                        counter[secLeg] = counter[secLeg] - 1
                        ls1.append(i)
                        ls2.append(secLeg)
                        break
                    elif counter[secLeg] ==0:
                        if j == (rowPossible-1):
                            ls1.append(i)
                            ls2.append("")
                            break
                        continue               
            except:
                ls1.append(i)
                ls2.append("")
        else:
            continue

    z = [i for i in zip(ls1,ls2)]
    ls1.extend(ls2)

    counter1= collections.Counter(ls1)
    print("List of Tzone Suggest: ",counter1)

    for i in counterOri.keys():
        gap = counterOri[i] - counter1[i]
        #print("Gap between Tzone Origin and Suggets {}/ {}".format(i,gap))
    
    return z,dfFinal

def setdata_tzone_matched(active_dir,df_morning, df_afternoon):
    
    ##### Get Filtering Data such as FourSubmitShipTo and Afternoon T-Zone #####
    dicts = get_filtering_data(active_dir)
    print(dicts[1]['FourSubmitShipTo'])
    
    print("df_morning",df_morning)

    ##### Get Moning Data and Rename, Fillna #####
    df_morning = df_morning[(~df_morning['Transporter Code'].isnull())]
    df_morning = df_morning[['Transportation Zone','Transporter Code','PreDONo','MergeQual','TransCode_text']]
    df_morning = df_morning.rename(columns = {'Transporter Code':'Transporter'})
    df_morning = df_morning.rename(columns = {'Transportation Zone':'Tzone'})   
    df_morning = df_morning.fillna('NaN')

    ##### Get Afternoon Data and Rename, Fillna, Excluded FourSubmit ShipTo #####
    df_afternoon = df_afternoon[(df_afternoon['TransCode_text'].isnull())]
    df_afternoon = df_afternoon[['Transportation Zone','PreDONo','MergeQual','ShipToCode','TransCode_text']]
 
    df_afternoon['ShipToCode'] = df_afternoon['ShipToCode'].str[1:]
    df_afternoon = df_afternoon[~df_afternoon['ShipToCode'].isin(dicts[1]['FourSubmitShipTo'])]

    print(dicts[1]['FourSubmitShipTo'])
    print(df_afternoon)
    
    df_afternoon = df_afternoon.rename(columns = {'Transportation Zone':'Tzone2'})
    df_afternoon = df_afternoon.fillna('NaN')
    print(df_afternoon)

    ##### Concat Morning and Afternoon to Dataframe for Matching T-Zone #####
    df_allday =  pd.concat([df_morning, df_afternoon], axis=1)
    df_allday.reset_index(drop = True,inplace = True)

    z, dfFinal = TzoneMatched(active_dir+'\\6W_Template_MatchedRoute.xlsx',df_allday)
    
    ##### Create T-Zone Mapping Datafram structure #####
    df_new = pd.DataFrame()
    reserved_cpdo = []      
    reserved_cpdo = {}
    merge_cpdo = {}
    reserved_cpdo['morning'] = '0'
    reserved_cpdo['afternoon'] = '0'
    merge_cpdo['morning'] = '0'
    merge_cpdo['afternoon'] = '0'
    
    ##### Get T-Zone Mapping to Dataframe #####
    for index, r_final in dfFinal.iterrows():
        ### Set up possibility ###
        if r_final['Possibility'] < 0.04: break 
        for index, r_morning in df_morning.iterrows():
            if str(r_morning['Tzone'])  == str(r_final['FirstLeg']) and str(r_morning['PreDONo']) not in reserved_cpdo['morning'] and str(r_morning['MergeQual']) not in merge_cpdo['morning']:
                for index, r_afternoon in df_afternoon.iterrows():                 
                    if str(r_afternoon['Tzone2'])  == str(r_final['SecondLeg']) and str(r_afternoon['PreDONo']) not in reserved_cpdo['afternoon'] and str(r_afternoon['MergeQual']) not in merge_cpdo['afternoon'] and str(r_afternoon['TransCode_text']) == 'NaN':   
                        new_data = (r_final['FirstLeg'],r_final['SecondLeg'],str(r_final['Possibility']),r_morning['PreDONo'],r_morning['Transporter'],str(r_afternoon['PreDONo']))  
                        df_new = df_new.append([new_data])
                        reserved_cpdo['morning'] = reserved_cpdo['morning'] +","+ str(r_morning['PreDONo'])
                        reserved_cpdo['afternoon'] = reserved_cpdo['afternoon'] +","+ str(r_afternoon['PreDONo'])
                        if str(r_morning['MergeQual']) != 'NaN':
                            merge_cpdo['morning'] = merge_cpdo['morning'] +","+ str(r_morning['MergeQual'])
                        if str(r_afternoon['MergeQual']) != 'NaN':
                            merge_cpdo['afternoon'] = merge_cpdo['afternoon'] +","+ str(r_afternoon['MergeQual'])
                        break
                    else:
                        continue                 
            else:
                continue
    
    ##### Print Data for Checking #####
    print('reserved_cpdo',reserved_cpdo['afternoon'])
    if df_new.empty:
        print('Not Found Mapping T-Zone!')
    else:    
        df_new.columns = ['FirstLeg','SecondLeg','Possibility','MorningCPDO','TransCode_text','AfternoonCPDO']
        df_new.reset_index(drop = True,inplace = True)
    
    print('Mapping T-Zone Complated',df_new)
    time.sleep(5)

    return df_new

def splitdata_tzone_matched(result_dir,df,DateFrom):
    from datetime import timedelta, datetime
    ################# Set Date Time #################################
    #Set Today Date and Format
    
    if DateFrom == '':
        D = datetime.now() - timedelta(days=0)
        D1 = D.strftime("%d%B%y")
        D2 = D.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        D1 = datetime.strptime(DateFrom, '%Y-%m-%d %H:%M')
        D2 = D1.replace(hour=12, minute=0, second=0, microsecond=0)

    ### Set Date Time Format ###
    D2 = D2 - timedelta(days=-1)
    M1 = D1.strftime("%Y_%m%B")
    T1 = D1.strftime("%H")

    print("Testing Date",D1)
    print ("D2 Cut Off Date Time = ",D2)
    print("Current Hour : ",T1,":00")
    
    ### Get Filtering Data to Dicts ###
    dicts = get_filtering_data(result_dir)

    ############### Filter Data to Morning and Afternoon will Use only Afternoon Shipment ###################
    df= df[(df['Logistics Region Group'] != 'NORTHEAST')]

    ## Morning T-Zone get East + Request Date Today Morning - Afternoon T-Zone
    df['Request From Date From'] = pd.to_datetime(df['Request From Date From'], format='%d-%m-%Y %H:%M')
    df_morning = df[(df['Logistics Region Code'] == 'EAST') | (df['Request From Date From'] < D2) & ~df['Transportation Zone'].isin(dicts[0]['AfternoonTZone'])]
          
    ## Afternoon T-Zone get all that still not Assign yet 
    df_afternoon = df[(df['Assign Date'].isnull())]
       
    print("Save File Already")    
    return df_morning, df_afternoon

def splitdata_tzone_morning(result_dir,df,DateFrom):
    from datetime import timedelta, datetime
    ################# Set Date Time #################################
    #Set Today Date and Format
    
    if DateFrom == '':
        D = datetime.now() - timedelta(days=0)
        D1 = D.strftime("%d%B%y")
        D2 = D.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        D1 = datetime.strptime(DateFrom, '%Y-%m-%d %H:%M')
        D2 = D1.replace(hour=12, minute=0, second=0, microsecond=0)

    ### Set Date Time Format ###
    D2 = D2 - timedelta(days=-1)
    M1 = D1.strftime("%Y_%m%B")
    T1 = D1.strftime("%H")

    print("Testing Date",D1)
    print ("D2 Cut Off Date Time = ",D2)
    print("Current Hour : ",T1,":00")
    
    ### Get Filtering Data to Dicts ###
    dicts = get_filtering_data(result_dir)

    ############### Filter Data to Morning and Afternoon will Use only Afternoon Shipment ###################
    df= df[(df['Logistics Region Group'] != 'NORTHEAST')]
    ## Morning T-Zone get East + Request Date Today Morning - Afternoon T-Zone
    df['Request From Date From'] = pd.to_datetime(df['Request From Date From'], format='%d-%m-%Y %H:%M')
    df_morning = df[(df['Logistics Region Code'] == 'EAST') | (df['Request From Date From'] < D2) & (~df['Transportation Zone'].isin(dicts[0]['AfternoonTZone']))]
       
    print("Save File Already")    
    return df_morning

def get_autoassign_data(active_dir):
    ##Get Filtering Data : Afternoon Shipment, Foursubmit Ship-To ####
    frames = pd.read_excel(active_dir+'\\Master_Template_AutoAssign.xlsx', sheet_name=None)
    dicts = [df.to_dict('list') for df in frames.values()]
    return dicts

def get_filtering_data(active_dir):
    ##Get Filtering Data : Afternoon Shipment, Foursubmit Ship-To ####
    frames = pd.read_excel(active_dir+'\\6W_Template_Filtering.xls', sheet_name=None)
    dicts = [df.to_dict('list') for df in frames.values()]
    return dicts

def suggest_log_conf_date(programpath,cpdo_province,logcontime_1,logcontime_2,add_day):
    #### Get Dicts for Add ConfirmDate ###
    from datetime import timedelta, datetime, date
    dicts = get_filtering_data(programpath)
    
    if str(cpdo_province) in str(dicts[2]['ConfirmDate+1'])  or str(cpdo_province) in str(dicts[3]['ConfirmDate+2']) or str(cpdo_province) in str(dicts[4]['ConfirmDate+3']) :
        #print("Found Province Code to Change SLA Date Time")
        
        if str(cpdo_province) in str(dicts[2]['ConfirmDate+1']):
            leadtime = 1
            D1 = date.today() + timedelta(days= leadtime + add_day)

        if str(cpdo_province) in str(dicts[3]['ConfirmDate+2']):
            leadtime = 2
            D1 = date.today() + timedelta(days= leadtime + add_day)

        if str(cpdo_province) in str(dicts[4]['ConfirmDate+3']):
            leadtime = 3
            D1 = date.today() + timedelta(days= leadtime + add_day)
            
        ###############################################################################
        logcondate_1 = datetime.strptime(logcontime_1, "%d-%m-%Y %H:%M")
        logcondate_2 = datetime.strptime(logcontime_2, "%d-%m-%Y %H:%M")
        ##### Adjust 12/03/2021 ### Fixed for Bag6W Area #####
        logcondate_1 = pd.to_datetime(str(D1)+' '+logcondate_1.strftime('%H:%M'))
        logcondate_2 = pd.to_datetime(str(D1)+' '+logcondate_2.strftime('%H:%M'))            
        
        ################################################################################    
        if logcondate_1 > logcondate_2:
            logcondate_1 = pd.to_datetime(str(D1)+' '+logcondate_1.strftime('%H:%M'))
            logcondate_2 = pd.to_datetime(str(D1+ timedelta(days= 1))+' '+logcondate_2.strftime('%H:%M'))
        
        logcontime_1 =  logcondate_1.strftime('%d-%m-%Y %H:%M:%S')
        logcontime_2 =  logcondate_2.strftime('%d-%m-%Y %H:%M:%S')
    
    
    else:
        logcontime_1 = logcontime_1
        logcontime_2 = logcontime_2
        
        if logcontime_1 > logcontime_2:
            logcontime_2 = pd.to_datetime(str(logcontime_1.strftime('%d-%m-%Y')+ timedelta(days= 1))+' '+logcontime_2.strftime('%H:%M'))
    
    return logcontime_1,logcontime_2,leadtime



def group_log_conf_date(programpath,cpdo_province,logcontime_1,logcontime_2,change_time="no"):
    #### Get Dicts for Add ConfirmDate ###
    from datetime import timedelta, datetime, date
    dicts = get_filtering_data(programpath)
    
    if str(cpdo_province) in str(dicts[2]['ConfirmDate+1'])  or str(cpdo_province) in str(dicts[3]['ConfirmDate+2']) or str(cpdo_province) in str(dicts[4]['ConfirmDate+3']) :
        #print("Found Province Code to Change SLA Date Time")
        if str(cpdo_province) in str(dicts[2]['ConfirmDate+1']):
            D1 = date.today() + timedelta(days= 1)

        if str(cpdo_province) in str(dicts[3]['ConfirmDate+2']):
            D1 = date.today() + timedelta(days= 2)
        
        if str(cpdo_province) in str(dicts[4]['ConfirmDate+3']):
            D1 = date.today() + timedelta(days= 3)
            
        ###############################################################################
        if change_time == "dbp_yes":
            logcondate_1 = pd.to_datetime(str(D1)+ ' 08:00')
            logcondate_2 = pd.to_datetime(str(D1)+ ' 17:00')
        elif change_time == "dbp_no":
            ##### Adjust 12/03/2021 ### Fixed for Bag6W Area #####
            logcondate_1 = datetime.strptime(logcontime_1, "%d-%m-%Y %H:%M")
            logcondate_2 = datetime.strptime(logcontime_2, "%d-%m-%Y %H:%M")            
        else:
            logcondate_1 = datetime.strptime(logcontime_1, "%d-%m-%Y %H:%M")
            logcondate_2 = datetime.strptime(logcontime_2, "%d-%m-%Y %H:%M")
            ##### Adjust 12/03/2021 ### Fixed for Bag6W Area #####
            logcondate_1 = pd.to_datetime(str(D1)+' '+logcondate_1.strftime('%H:%M'))
            logcondate_2 = pd.to_datetime(str(D1)+' '+logcondate_2.strftime('%H:%M'))            
            
            ################################################################################    
            if logcondate_1 > logcondate_2:
                logcondate_1 = pd.to_datetime(str(D1)+' '+logcondate_1.strftime('%H:%M'))
                logcondate_2 = pd.to_datetime(str(D1+ timedelta(days= 1))+' '+logcondate_2.strftime('%H:%M'))
        
        logcontime_1 =  logcondate_1.strftime('%d-%m-%Y %H:%M:%S')
        logcontime_2 =  logcondate_2.strftime('%d-%m-%Y %H:%M:%S')
    
    else:
        logcontime_1 = logcontime_1
        logcontime_2 = logcontime_2
        
        if logcontime_1 > logcontime_2:
            logcontime_2 = pd.to_datetime(str(logcontime_1.strftime('%d-%m-%Y')+ timedelta(days= 1))+' '+logcontime_2.strftime('%H:%M'))

    print(logcontime_1," : ",logcontime_2)

    return logcontime_1,logcontime_2

def change_log_conf_date(programpath,cpdo_province,logcontime_1,logcontime_2,driver):
    #### Get Dicts for Add ConfirmDate ###
    from datetime import timedelta, datetime, date
    dicts = get_filtering_data(programpath)
    print("Function Check Province Code for Change SLA Date Time")

    if str(cpdo_province) in str(dicts[2]['ConfirmDate+1'])  or str(cpdo_province) in str(dicts[3]['ConfirmDate+2']) or str(cpdo_province) in str(dicts[4]['ConfirmDate+3']) :
        #print("Found Province Code to Change SLA Date Time")
        if str(cpdo_province) in str(dicts[2]['ConfirmDate+1']):
            D1 = date.today() + timedelta(days= 1)

        if str(cpdo_province) in str(dicts[3]['ConfirmDate+2']):
            D1 = date.today() + timedelta(days= 2)
        
        if str(cpdo_province) in str(dicts[4]['ConfirmDate+3']):
            D1 = date.today() + timedelta(days= 3)
            
        logcontime_1 = datetime.strptime(logcontime_1, "%d-%m-%Y %H:%M")
        logcontime_2 = datetime.strptime(logcontime_2, "%d-%m-%Y %H:%M")
        
        if logcontime_1 <= logcontime_2:
            logcondate_1 = pd.to_datetime(str(D1)+' '+logcontime_1.strftime('%H:%M'))
            logcondate_2 = pd.to_datetime(str(D1)+' '+logcontime_2.strftime('%H:%M'))
        else:
            logcondate_1 = pd.to_datetime(str(D1)+' '+logcontime_1.strftime('%H:%M'))
            logcondate_2 = pd.to_datetime(str(D1+ timedelta(days= 1))+' '+logcontime_2.strftime('%H:%M'))     
        
        logcondate_1 =  logcondate_1.strftime('%d-%m-%Y %H:%M:%S')
        logcondate_2 =  logcondate_2.strftime('%d-%m-%Y %H:%M:%S')
        
        print(logcondate_1)
        print(logcondate_2)
        
        driver.find_element(By.ID, 'order_release/early_delivery_date::content').send_keys(logcondate_1)
        driver.find_element(By.ID, 'order_release/late_delivery_date::content').send_keys(logcondate_2)      
        
def change_log_conf_date_uat(programpath,cpdo_province,logcontime_1,logcontime_2,driver):
    #### Get Dicts for Add ConfirmDate ###
    from datetime import timedelta, datetime, date
    dicts = get_filtering_data(programpath)
    print("Function Check Province Code for Change SLA Date Time")

    if str(cpdo_province) in str(dicts[2]['ConfirmDate+1'])  or str(cpdo_province) in str(dicts[3]['ConfirmDate+2']) or str(cpdo_province) in str(dicts[4]['ConfirmDate+3']) :
        #print("Found Province Code to Change SLA Date Time")
        if str(cpdo_province) in str(dicts[2]['ConfirmDate+1']):
            D1 = date.today() + timedelta(days= 1)

        if str(cpdo_province) in str(dicts[3]['ConfirmDate+2']):
            D1 = date.today() + timedelta(days= 2)
        
        if str(cpdo_province) in str(dicts[4]['ConfirmDate+3']):
            D1 = date.today() + timedelta(days= 3)
            
        logcontime_1 = datetime.strptime(logcontime_1, "%d-%m-%Y %H:%M")
        logcontime_2 = datetime.strptime(logcontime_2, "%d-%m-%Y %H:%M")
                   
        if D1 > datetime.date(logcontime_1):
            print("New SLA Date  > Log Confirm Delivery Date : Change to NEW SLA Date")

            if logcontime_1 <= logcontime_2:
                logcondate_1 = pd.to_datetime(str(D1)+' '+logcontime_1.strftime('%H:%M'))
                logcondate_2 = pd.to_datetime(str(D1)+' '+logcontime_2.strftime('%H:%M'))
            else:
                logcondate_1 = pd.to_datetime(str(D1)+' '+logcontime_1.strftime('%H:%M'))
                logcondate_2 = pd.to_datetime(str(D1+ timedelta(days= 1))+' '+logcontime_2.strftime('%H:%M'))  
            
            logcondate_1 =  logcondate_1.strftime('%d-%m-%Y %H:%M:%S')
            logcondate_2 =  logcondate_2.strftime('%d-%m-%Y %H:%M:%S')
            
            print(logcondate_1)
            print(logcondate_2)
            
            driver.find_element(By.ID, 'order_release/early_delivery_date::content').send_keys(logcondate_1)
            driver.find_element(By.ID, 'order_release/late_delivery_date::content').send_keys(logcondate_2)           
        
def mass_update_tran(massUpdate_group,subshippingtype,driver):
    count = 0
    for key in massUpdate_group:
        try:
            print("key ---> ",key)
            sprovider = str(key)
            
            time.sleep(1)    
            #driver.switch_to.window(driver.window_handles[0])
            for handle in driver.window_handles:#9999
                driver.switch_to.window(handle)#9999       
            time.sleep(3)
            driver.switch_to.frame(1)

            wait = WebDriverWait(driver,30)
            element = wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@id='search_button']")))
            
            time.sleep(1)
            driver.find_element(By.NAME, "order_release/xid").send_keys(massUpdate_group[key])
            
            #Search Shipment by CPDO number
            dropdown = driver.find_element(By.NAME, "order_release/xid_operator")
            if "," in massUpdate_group[key]: 
                dropdown.find_element(By.XPATH, "//option[. = 'One Of']").click()
            else:
                dropdown.find_element(By.XPATH, "//option[. = 'Begins With']").click()
            
            driver.find_element(By.ID, "search_button").click() 
            print (key, 'massUpdate_group Key -> ',massUpdate_group[key])
            
            count = massUpdate_group[key].count(",")
            count = int(count) + 2
            #Check Missing CPDO
            try:
                element=driver.find_element_by_id("rgNoDataMsg")
                print ("No Data Found")
                driver.refresh()
                continue#99999
            
            except:
                element=driver.find_element_by_name("rgSGSec.2.1.1.3IndicatorImg")
                print ("Data Found")

        
            element = wait.until(EC.element_to_be_clickable((By.XPATH,'//span[contains(text(),"Records")]'))) 
            driver.find_element(By.ID, 'rgSGSec.1.1.1.1.check').click()
            #################### Fist Task Mass Update Transporter Code and Sub ShippingType ###########################    
            #Click Mass Update Icon
            time.sleep(2)    
            driver.find_element(By.ID, 'rgMassUpdateImg').click()
            
            #Fill in Transporter Code and Sub ShppingType
            time.sleep(1)
            driver.switch_to.frame(2)
            time.sleep(2)
             
            ###################### Get CPDO whith Merge Quantity ##########################            
            driver.find_element(By.ID, 'order_release/equipment_group_xid').send_keys(subshippingtype)
            driver.find_element(By.XPATH, "//input[@id='order_release/servprov_xid']").send_keys(sprovider)
            
            # Click OK and Pop-Up windows
            time.sleep(1)
            driver.switch_to.window(driver.window_handles[0])
            driver.switch_to.frame(1)
            
            time.sleep(1)
            driver.find_element(By.XPATH, "//button[@id='resultsPage:MassUpdatePopupDialog::save']").click()
            time.sleep(count + 5)#9999
            driver.find_element(By.ID, "saveClose").click()    
            
            driver.refresh()
            time.sleep(3)
        except:
            print("Process Fail @ Mass Update Group : ", key)
            continue          

def mass_update_group(massUpdate_group,subshippingtype,driver):
    count = 0
    for key in massUpdate_group:
        try:
            print("key ---> ",key)
            sprovider = str(key).split('/')[0]
            logcontime_1 = str(key).split('/')[1]
            logcontime_2 = str(key).split('/')[2]
            
            time.sleep(1)    
            #driver.switch_to.window(driver.window_handles[0])
            for handle in driver.window_handles:#9999
                driver.switch_to.window(handle)#9999       
            time.sleep(3)
            driver.switch_to.frame(1)

            wait = WebDriverWait(driver,30)
            element = wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@id='search_button']")))
            
            time.sleep(1)
            driver.find_element(By.NAME, "order_release/xid").send_keys(massUpdate_group[key])
            
            #Search Shipment by CPDO number
            dropdown = driver.find_element(By.NAME, "order_release/xid_operator")
            if "," in massUpdate_group[key]: 
                dropdown.find_element(By.XPATH, "//option[. = 'One Of']").click()
            else:
                dropdown.find_element(By.XPATH, "//option[. = 'Begins With']").click()
            
            driver.find_element(By.ID, "search_button").click() 
            print (key, 'massUpdate_group Key -> ',massUpdate_group[key])
            
            count = massUpdate_group[key].count(",")
            count = int(count) + 1
            #Check Missing CPDO  
            try:
                element=driver.find_element_by_id("rgNoDataMsg")
                print ("No Data Found")
                driver.refresh()
                continue#99999                
                
            except:
                element=driver.find_element_by_name("rgSGSec.2.1.1.3IndicatorImg")
                print ("Data Found")
                if int(count) > 25:
                    try:
                        wait = WebDriverWait(driver,20)
                        element = wait.until(EC.element_to_be_clickable((By.XPATH,"//a[contains(text(),'All')]"))) 
                        element.click()
                        time.sleep(5)
                    except:
                        pass

            element = wait.until(EC.element_to_be_clickable((By.XPATH,'//span[contains(text(),"Records")]'))) 
            driver.find_element(By.ID, 'rgSGSec.1.1.1.1.check').click()
            #################### Fist Task Mass Update Transporter Code and Sub ShippingType ###########################    
            #Click Mass Update Icon
            time.sleep(2)    
            driver.find_element(By.ID, 'rgMassUpdateImg').click()
            
            #Fill in Transporter Code and Sub ShppingType
            time.sleep(1)
            driver.switch_to.frame(2)
            time.sleep(2)
             
            ###################### Get CPDO whith Merge Quantity ##########################         
            driver.find_element(By.ID, 'order_release/early_delivery_date::content').send_keys(logcontime_1)
            driver.find_element(By.ID, 'order_release/late_delivery_date::content').send_keys(logcontime_2)        
            driver.find_element(By.ID, 'order_release/equipment_group_xid').send_keys(subshippingtype)
            driver.find_element(By.XPATH, "//input[@id='order_release/servprov_xid']").send_keys(sprovider)
            
            # Click OK and Pop-Up windows
            time.sleep(1)
            driver.switch_to.window(driver.window_handles[0])
            driver.switch_to.frame(1)
            
            time.sleep(1)
            driver.find_element(By.XPATH, "//button[@id='resultsPage:MassUpdatePopupDialog::save']").click()
            time.sleep(count + 5)#9999 @ 03/10/2020
            driver.find_element(By.ID, "saveClose").click()    
            
            driver.refresh()
            time.sleep(3)
        except:
            print("Process Fail @ Mass Update Group : ", key)
            continue          

def shipment_tendor(cpdoall,driver):
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    
    time.sleep(1)
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
    
    count = cpdoall.count(",")
    count = int(count)    

    ##### Click Menu Shipment > Planned #####
    time.sleep(8)
    driver.find_element(By.ID, 'homecanvas').click()
    time.sleep(5)
    driver.find_element_by_link_text("Shipments").click()
    time.sleep(3)
    
    try:
        driver.find_element_by_link_text("Planned").click()
    except:
        driver.find_element_by_link_text("Shipments").click()
        time.sleep(6)
        driver.find_element_by_link_text("Planned").click()

    #Load Page and Click Tab Order Release
    driver.switch_to.window(driver.window_handles[0])

    time.sleep(2)
    driver.switch_to.frame(1)
    
    time.sleep(3)
    driver.find_element(By.LINK_TEXT, "Order Release").click()
    time.sleep(2)
    driver.find_element(By.ID, "shipment/order_rel_xid").send_keys(cpdoall)
    driver.find_element(By.NAME, "shipment/order_rel_xid_operator").click()
    dropdown = driver.find_element(By.NAME, "shipment/order_rel_xid_operator")

    #Search Shipment by CPDO number
    if "," in cpdoall:
        dropdown.find_element(By.XPATH, "//option[. = 'One Of']").click()
    else:
        dropdown.find_element(By.XPATH, "//option[. = 'Begins With']").click()
        
    driver.find_element(By.NAME, "search_button").click()
    time.sleep(int(count)+5)

    #Check Shipment Before Tendor to Transporter
    try:
        element=driver.find_element_by_id("rgNoDataMsg")
        print ("No Data Found")
        
    except:
        print ("Data Found")
        if int(count) > 25:
            try:
                wait = WebDriverWait(driver,20)
                element = wait.until(EC.element_to_be_clickable((By.XPATH,"//a[contains(text(),'All')]"))) 
                element.click()
            except:
                pass

        wait = WebDriverWait(driver,20)
        element = wait.until(EC.element_to_be_clickable((By.XPATH,'//span[contains(text(),"Records")]')))
        time.sleep(3)
        driver.find_element(By.ID, 'rgSGSec.1.1.1.1.check').click()
          
        driver.switch_to.window(driver.window_handles[0])
        time.sleep(1)
        driver.switch_to.frame(1)
        
        # Click Side Menu Tree : Waiting unitl menu show
        driver.find_element(By.CSS_SELECTOR, ".finderActionButtonImg").click()
        wait = WebDriverWait(driver,20)
        element = wait.until(EC.frame_to_be_available_and_switch_to_it("actionFrame"))
        
        # Click Side Menu Tree Transporter Assignment
        time.sleep(2)
        try:
            driver.find_element_by_xpath("//*[@id='actionTree.1_2_5_1.k']").click()
        except:
            driver.find_element_by_xpath("//*[@id='actionTree.1_1_5_1.k']").click()
        # Close Bulk Plan Pop-up Windows
        time.sleep(int(count)+5)
        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        time.sleep(3)

    print("Transporter Assingment Completed")

def set_screen(x,y):
    import win32api
    import win32con
    import pywintypes

    devmode = pywintypes.DEVMODEType()

    devmode.PelsWidth = x
    devmode.PelsHeight = y

    devmode.Fields = win32con.DM_PELSWIDTH | win32con.DM_PELSHEIGHT

    win32api.ChangeDisplaySettings(devmode, 0)

def save_history(detail,file,error):
    import Tea as t
    subject = 'Auto_Assign_Step2 : '+ detail.rsplit(".",1)[0]
    try:
        df_his=t.history() #['subject','detail','FileOutput','error','SendMail']
        df_his.add([subject,detail,file,error,'Send Mail to LCC Already']) #to add record
        df_his.save() #To save into DW
    except:
        pass
    
def save_history_other(detail,file,error):
    import Tea as t
    subject = detail.rsplit(".",1)[0]
    try:
        df_his=t.history() #['subject','detail','FileOutput','error','SendMail']
        df_his.add([subject,detail,file,error,'Send Mail to LCC Already']) #to add record
        df_his.save() #To save into DW
    except:
        pass

def password_encode(configFilePath,config_group_name,config_password_name):
    import configparser
    from cryptography.fernet import Fernet

    #Fernet.generate_key()
    key =   b'sccc11112222333344445555666677888899990000x='
    cipher_suite = Fernet(key)

    ##### Get Data From Config File #####
    config = configparser.ConfigParser()
    #configFilePath = r'D:\PyThon\config.ini'
    config.read(configFilePath)
    password = config.get(config_group_name,config_password_name)

    if len(password) >= 100:
        #print("Password has been Encrypt already")
        password = bytes(password, "utf-8")
        password2 = cipher_suite.decrypt(password)
        password2 = password2.decode("utf-8")
        #print("Encode Password : ",password)
        password = password2
    else:
        #print("Password has not been Encrypt yet!")
        cipher_text = cipher_suite.encrypt(password.encode())
        #Get the USERINFO section and Update the password
        userinfo = config[config_group_name]
        userinfo[config_password_name] = cipher_text.decode("utf-8")
        with open(configFilePath, 'w') as conf:
            config.write(conf)
            #print("Write Encode Password :",cipher_text)
    
    return password

class Logger(object):
    def __init__(self, filename="Red.Wood", mode="a", buff=1):
        self.stdout = sys.stdout
        self.file = open(filename, mode, buff)
        sys.stdout = self

    def __del__(self):
        self.close()

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def write(self, message):
        self.stdout.write(message)
        self.file.write(message)

    def flush(self):
        self.stdout.flush()
        self.file.flush()
        os.fsync(self.file.fileno())

    def close(self):
        if self.stdout != None:
            sys.stdout = self.stdout
            self.stdout = None

        if self.file != None:
            self.file.close()
            self.file = None
            
def TooAssign(self,df_FleetCap,priority,pmDict,TransList,df_PriorityMark,PriorityMap):
       
    try:
        PriorityMark_tran = df_PriorityMark.loc[df_PriorityMark['priority'] == priority, 'Transporter Code'].iloc[0]
    except:
        df_PriorityMark = pd.DataFrame()
        df_PriorityMark = df_PriorityMark.append({'priority': priority,'Transporter Code': '2100000'}, ignore_index=True)
        PriorityMark_tran = ""
        
    print ("df_PriorityMark_tran >>>>>>>>>>>>>>>>> : ",df_PriorityMark)   
    
    ##### Initial Data #####        
    fleet_cap_copy = dict(zip(df_FleetCap['Transporter Code'], df_FleetCap['FleetCap']))
    self_new = self.copy()
    self_new = self_new[(self_new['Assign Date'].isnull())]
    self_new = self_new[(self_new[PriorityMap].notnull())] ###<<< Adjust 21/12/2020

    ###VVVV Adjust 22/12/2020 VVVV###
    if PriorityMap[:14] == 'TransCode_prov' :
        self_new = self_new[(self_new['MapLevel'] == 'Province')] 
    elif PriorityMap[:14] == 'TransCode_ship':    
        self_new = self_new[(self_new['MapLevel'] == 'ShipToCode')]
    elif PriorityMap[:14] == 'TransCode_sold':    
        self_new = self_new[(self_new['MapLevel'] == 'SoldToCode')] 
    elif PriorityMap[:14] == 'TransCode_tzon':    
        self_new = self_new[(self_new['MapLevel'] == 'Transportation Zone')]
        
    ###VVVV Adjust 27/01/2021 VVVV###    
    converted_list = [str(element) for element in TransList]
    joined_string = "/".join(converted_list)
    print("joined_string : ",joined_string)
    self_new = self_new[(self_new['Transporter Code'] == joined_string)]
    print("self_new : ",self_new)

    cpdo = self_new['MergeQual'].tolist()
    criteria = self_new['criteria'].iloc[0]
    print("#### criteria >>>>>>>>> ",criteria)
    
    ### Save Console Data to Logfile ###
    uat_path = (os.path.abspath(os.path.join(os.path.dirname(__file__), '..\\auto_assign\\UAT\\'))) 
    logfile = criteria + "_" + datetime.datetime.now().strftime("%d%m%Y-%H%M%S")+'_log.txt' 
    my_console = Logger(uat_path+"\\"+logfile)

    print("#### criteria >>>>>>>>> ",criteria)
    print('#######################################Call Too Function#######################################')
    print("self ",self)
    print("self new",self_new)
    print("priority : ",priority)
    print("pmDict : ",pmDict)
    print("TransList",TransList)
    print("PriorityMap  >>>>>>>>>>>>>> ",PriorityMap)
    print("PriorityMap_Key  >>>>>>>>>>>>>> ",PriorityMap[:14])

    try:
        priority_fleet_cap = pmDict[priority]
    except:
        priority_fleet_cap = 100
    
    try:
        #df_assigned['Transporter Code'] = df_assigned['Transporter Code'].astype(str).replace('\.0', '', regex=True)
        count_tran_assigned = self.groupby(['Transporter Code'])['Assign Date'].count().to_dict()       
    except:
        count_tran_assigned = {'2100000': 0}
        
    print("count_tran_assigned : ",count_tran_assigned)
    print("fleet_cap_copy : ",fleet_cap_copy)      
    
    
    fleet_cap = ({k: fleet_cap_copy.get(k, 0) - count_tran_assigned.get(k, 0) for k in set(fleet_cap_copy) | set(count_tran_assigned)})
    print("fleet_cap",fleet_cap)
    
    ##### Add FleetCap Assign Before to DF FleetCap #####
    #df_FleetCap['before'] = df_FleetCap['Transporter Code'].apply(lambda x: count_tran_assigned.get(x)).fillna('')
    for index in df_FleetCap.index:
        try:
            df_FleetCap.loc[index,'before']=count_tran_assigned[df_FleetCap['Transporter Code'][index]]
        except:
            df_FleetCap.loc[index,'before']=''

    ##### Re-arrange Transporter List by Last Assign #####
    try:
        my_index = TransList.index(str(PriorityMark_tran))
        TransList = TransList[my_index + 1:] + TransList[:my_index + 1]
    except:
        pass
    print(TransList)

    ##### Select Transporter Code with Fleet Cap > 0 #####
    TransList_new = []
    for i, x in enumerate(fleet_cap[x] for x in TransList):
        if x > 0:
            TransList_new = TransList_new + [TransList[i]]
    TransList = TransList_new

    ##### Count of Data #####
    count_tran = len(TransList)
    count_cpdo = len(cpdo)
    fleet_cap_count = {key: 0 for key in fleet_cap}
    tran_assign_count = {}
    
    ### Print Data for Checking #####
    print("priority_fleet_cap : ",priority_fleet_cap)
    print ('count_cpdo',count_cpdo)
    print(TransList)
    print(cpdo) 
    
    while priority_fleet_cap > 0:
        full_fleetcap = False
        final_round = False
        reserved_cpdo_count = 1
        TransList_del_list = []
        for j in range(1,count_cpdo+1):
            round = 0
            #time.sleep(0.5)
            if final_round == True:
                print("----------- Start New Round ----------")
                print(TransList)
                print(cpdo)
                break

            for i in range(1,count_tran+1):
                round = round + 1
             
                if int(i) == int(j % count_tran) and fleet_cap_count[TransList[i-1]] < fleet_cap[TransList[i-1]] :
                    print ("Transporter : ",i,"/",TransList[i-1],"and CPDO :",j,"/",cpdo[j-(reserved_cpdo_count)])

                    if TransList[i-1] not in tran_assign_count.keys():    
                        tran_assign_count[TransList[i-1]] = 1
                    else:
                        tran_assign_count[TransList[i-1]] = int(tran_assign_count[TransList[i-1]]) + 1
                        
                    #Save Data Back to Self Dataframe
                    self.loc[self['MergeQual'] == cpdo[j-(reserved_cpdo_count)], 'Transporter Code'] = TransList[i-1]
                    self.loc[self['MergeQual'] == cpdo[j-(reserved_cpdo_count)], 'priority'] = priority
                    self.loc[self['MergeQual'] == cpdo[j-(reserved_cpdo_count)], 'N'] = tran_assign_count[TransList[i-1]]
                    df_FleetCap.loc[df_FleetCap['Transporter Code'] == TransList[i-1], 'actual'] = tran_assign_count[TransList[i-1]]
                    df_PriorityMark.loc[df_PriorityMark['priority'] == priority, 'Transporter Code'] = TransList[i-1]
                    if fleet_cap_count[TransList[i-1]] == fleet_cap[TransList[i-1]]-1:
                        self.loc[self['MergeQual'] ==cpdo[j-(reserved_cpdo_count)], 'REMARK'] = "Last CPDO"
                        print("TransList[i-1] A",TransList[i-1])
                        df_FleetCap.loc[df_FleetCap['Transporter Code'] == TransList[i-1], 'isFullCap'] = "TRUE"
                    
                    priority_fleet_cap = priority_fleet_cap - 1
                    cpdo.remove(cpdo[j-(reserved_cpdo_count)])
                    reserved_cpdo_count = reserved_cpdo_count + 1
                    fleet_cap_count[TransList[i-1]] = fleet_cap_count[TransList[i-1]] + 1
                    count_cpdo = len(cpdo)

                if round == count_tran and int(j % count_tran) == 0:
                    if fleet_cap_count[TransList[i-1]] <= fleet_cap[TransList[i-1]]:
                        print ("Transporter : ",i,"/",TransList[i-1],"and CPDO :",j,"/",cpdo[j-(reserved_cpdo_count)])

                        if TransList[i-1] not in tran_assign_count.keys():    
                            tran_assign_count[TransList[i-1]] = 1
                        else:
                            tran_assign_count[TransList[i-1]] = int(tran_assign_count[TransList[i-1]]) + 1

                        #Save Data Back to Self Dataframe
                        self.loc[self['MergeQual'] == cpdo[j-(reserved_cpdo_count)], 'Transporter Code'] = TransList[i-1]
                        self.loc[self['MergeQual'] == cpdo[j-(reserved_cpdo_count)], 'priority'] = priority
                        self.loc[self['MergeQual'] == cpdo[j-(reserved_cpdo_count)], 'N'] = tran_assign_count[TransList[i-1]]
                        df_FleetCap.loc[df_FleetCap['Transporter Code'] == TransList[i-1], 'actual'] = tran_assign_count[TransList[i-1]]
                        df_PriorityMark.loc[df_PriorityMark['priority'] == priority, 'Transporter Code'] = TransList[i-1]
                        if fleet_cap_count[TransList[i-1]] == fleet_cap[TransList[i-1]]-1:
                            self.loc[self['MergeQual'] ==cpdo[j-(reserved_cpdo_count)], 'REMARK'] = "Last CPDO"
                            print("TransList[i-1] B",TransList[i-1])
                            df_FleetCap.loc[df_FleetCap['Transporter Code'] == TransList[i-1], 'isFullCap'] = "TRUE"                                

                        priority_fleet_cap = priority_fleet_cap - 1
                        print ("--------------------------------------")
                        cpdo.remove(cpdo[j-(reserved_cpdo_count)])
                        reserved_cpdo_count = reserved_cpdo_count + 1
                        fleet_cap_count[TransList[i-1]] = fleet_cap_count[TransList[i-1]] + 1
                        count_cpdo = len(cpdo)
                        
                if fleet_cap_count[TransList[i-1]] == fleet_cap[TransList[i-1]]:
                    TransList_del_list = TransList_del_list+[TransList[i-1]]
                    TransList_del_list = list(set(TransList_del_list))
                    full_fleetcap = True
                        
                if round == count_tran and int(j % count_tran) == 0 and full_fleetcap == True or priority_fleet_cap <= 0 :
                    print ("TransList_del : ", TransList_del_list," : Full Fleet Cap")
                    for j in (TransList_del_list):
                        TransList.remove(j)
                    count_tran = len(TransList)
                    final_round = True
                    break

            if priority_fleet_cap <= 0 :
                print("priority_fleet_cap",priority_fleet_cap)
                break

        if count_tran == 0 or count_cpdo == 0 or priority_fleet_cap <= 0 :
            break

    print("#### Finished #####")
    
    my_console.close()
    
    return df_FleetCap,df_PriorityMark

def add_valiable_to_sql(sql_template,sql_result,valiable_data):
    """ Example to use this code 
    sql_template = 'D:\PyThon\Shipment_cost_by_do_users_Too.sql'
    sql_result = 'D:\PyThon\Shipment_cost_by_do_users_Too1.sql'
    valiable_data = {"CPDO_List":"'7777','8888','3333','4444','5555','6666'"}
    pt.add_valiable_to_sql(sql_template,sql_result,valiable_data)
    """
    from jinja2 import Environment
    
    with open(sql_template, 'r') as source_file:
        sql = source_file.read().replace('\n', '')

    sql_new = Environment().from_string(sql).render(valiable_data)
    
    with open(sql_result, 'w') as result_file:
        result_file.truncate()
        result_file.write(sql_new)
        
def convertXLS(file_name):
    import pandas as pd
    sheet_df = pd.read_csv(file_name,sep='\t',encoding='utf-16 le')  
    writer = pd.ExcelWriter(file_name+"x")
    sheet_df.to_excel(writer, output_name, index=False)
    writer.save()
    writer.close()

def get_ontime_penalty(active_dir,output_name='Penalty',isSaveHistory='No'):
    import gspread    
    from oauth2client.service_account import ServiceAccountCredentials
    from datetime import timedelta, date
    pd.options.mode.chained_assignment = None  # default='warn'
    
    #os.chdir(active_dir)
    print("New_active_dir",active_dir)

    ######################## Set Date Time and Template File Name ################################
    D1 = date.today() - timedelta(days=0)
    M1 = D1.strftime("%Y_%m%b")
    D1 = D1.strftime("%d-%m-%Y")
    
    file_name = active_dir+r'\\UAT\\ontime_penalty_UAT_'+M1+'.xlsx'
    
    ########## Read Transporter Code and Shipping Type from Template #########################
    google_sheet = '21000XX_Penalty_waiver_request'
    ########################## Get Data From Google Sheet ####################################
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    # Your json file here
    credentials = ServiceAccountCredentials.from_json_keyfile_name(active_dir + r'\zinc-proton-268015-d2cf843be317.json', scope)
    gc = gspread.authorize(credentials)

    while True:
        try:
            wks = gc.open(google_sheet).sheet1
            #sheet1 = gc.get_worksheet(0)
            
            #num_rows=len(wks.row_values(1))                
            data = wks.get_all_values()
            status = True
        except:
            time.sleep(5)
            print("Not Found", google_sheet)
        break

    headers = data.pop(0)
    df_gd = pd.DataFrame(data, columns=headers)
    row_used = len(df_gd.index)
    
    for i in range(2,int(row_used)):
        wks.update_cell(i, 12, 1)
    
    cols = ['Timestamp','ShipmentID','เหตุผลที่ขออนุโลมค่าปรับ_On-Time','แนบหลักฐานที่อ้างอิงถึงเหตุผลที่ขออนุโลมค่าปรับ_On-Time','เหตุผลที่ขออนุโลมค่าปรับ_ILM','แนบหลักฐานที่อ้างอิงถึงเหตุผลที่ขออนุโลมค่าปรับ_ILM','วันที่พบปัญหาการใช้งาน','รายละเอียดเพิ่มเติมในการขออนุโลมค่าปรับ_On-Time_(ผู้ขนส่งกรอก-ถ้ามี)','ผลการตรวจสอบ_(LCC)','เพิ่มเติมเหตุผลการตรวจสอบ_LCC','IsDBupdate']
    df_pm = df_gd[cols]
    df_pm.columns = ['UPDATE_DATE','ShipmentID','OT_Reason','OT_Evidence','ILM_Reason','ILM_Evidence','FoundDate','Reason_Additonal','LCC_Approval','LCC_Reason','IsDBupdate']
    df_pm.insert(2, 'UPDATE_BY', os.getlogin() )
    df_pm.insert(2, 'Evidence', "")
    df_pm.insert(2, 'Reason', "")
    df_pm.insert(2, 'Criteria', "")  

    for index, row in df_pm.iterrows():
        
        if str(row['OT_Reason']) == "":
            df_pm.loc[index, 'Criteria'] = "ILM"
            df_pm.loc[index, 'Reason'] = df_pm.loc[index, 'ILM_Reason']
            df_pm.loc[index, 'Evidence'] = df_pm.loc[index, 'ILM_Evidence']
        else:
            df_pm.loc[index, 'Criteria'] = "OT"
            df_pm.loc[index, 'Reason'] = df_pm.loc[index, 'OT_Reason']
            df_pm.loc[index, 'Evidence'] = df_pm.loc[index, 'OT_Evidence']

    writer = pd.ExcelWriter(file_name)
    df_pm.to_excel(writer, output_name, index=False)
    print("Write Data to Excel already") 
    writer.save()

    cols = ['ShipmentID','Criteria','FoundDate','Reason','Evidence','Reason_Additonal','LCC_Approval','LCC_Reason','IsDBupdate','UPDATE_DATE','UPDATE_BY']
    df_pm = df_pm[cols]

    return df_pm    


####################### End of All Functions  ######################



