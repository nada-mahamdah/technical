import requests
import json
import os.path, time
import pymysql.cursors
import datetime

group_id = 3712499

generate_holiday_plan_report = True
generate_pay_type_hourly_employee_report = False
generate_pay_type_salaried_employee_report = False  # this includes weighted shift as well
generate_LMS_report=False

Mycookie = {"session": "602519b6fd9360f75e78bb92"}



########################################################################################
########################################################################################
########################################################################################
today = datetime.date.today()
try:
    connection = pymysql.connect(host='35.171.94.111',
                                 port=3307,
                                 user='nada.mahamdah',
                                 password='tPKxF?U4$H^NF^fN',
                                 db='harri3_live',
                                 cursorclass=pymysql.cursors.DictCursor);








    with connection.cursor() as cursor1:
        cursor1.execute("set @group_id= "+str(group_id)+";");
        brands=[]
        d={}
        sql="select id,replace(name,',',' ') as name from brand where group_id = @group_id union select id,replace(name,',',' ') from brand where id = @group_id";
        row=cursor1.execute(sql)
        for row in cursor1:
            brand_id=(row['id'])
            brand_name=(row['name'])
            d[brand_id]=brand_name



except Exception as e:
                print e.message, e.args

try:

    connection = pymysql.connect(host='35.171.94.111',
                                 port=3309,
                                 user='nada.mahamdah',
                                 password='A6SD8/m,]x_!kHPQ',
                                 db='team',
                                 cursorclass=pymysql.cursors.DictCursor);


    if generate_holiday_plan_report == True:

        headers = {'Content-Type': 'application/json', 'Accept': 'application/json, text/javascript, */*; q=0.01'}
        url1 = 'https://api.harri.com/admin/impersonate_brand/' + str(group_id)
        r1 = requests.get(url1, headers=headers, cookies=Mycookie)
        time.sleep(3)

        f = open("/Users/imadkhuffash/Desktop/desktop/reports/" + str(d[group_id]) + " " + str(today) + " holiday_report.csv", "aw+")
        f.truncate(0)
        f.write('user_id,full name,Email,Brand_id,Brand_name,is_primary,Holiday Plan type,Holiday Plan name,holiday plan start date,accruals,adjustments,booked,taked,allowance,ATD,full_annual_entitlement,remaining_annual_balance,daily_paid,total_worked_days,daily_worked,\r\n')

        with connection.cursor() as cursor:
            cursor.execute("set @group_id= "+str(group_id)+";");
            cursor.execute("select @brand_year := start_date from team_attendance.brand_holiday_year where brand_id = @group_id;");
            cursor.execute("set @var1=STR_TO_DATE(concat(year(now()),substr(FROM_UNIXTIME(@brand_year),5,6)),'%Y-%m-%d');");
            cursor.execute("select if(@var1 > NOW(),@final :=  date_sub(@var1, interval 1 year) ,@final := @var1);");
            cursor.execute("select @eofy := date_add(@final,interval 1 year);");
            #sql="select user.id,concat(first_name,' ',last_name) as full_name,email,harri_brand_id,brand_holiday_plan.type,brand_holiday_plan.title as holiday_plan_name,user_holiday_plan.adjustments,round(user_holiday_plan.accruals,2) as accruals1 ,user_holiday_plan.start_date as user_holiday_plan_start_date,ifnull(taken.taken_days,0) as taken_days,IFNULL (booked_days.booked_days,0) as booked_days,(round(user_holiday_plan.accruals-taken_days,2)) as ATD from user JOIN brand_user on user.id = brand_user.user_id left join ( select id,user_id,accruals,brand_holiday_plan_id,max(start_date) as start_date,adjustments from team_attendance.user_holiday_plan where deleted = 0 group by user_id) as user_holiday_plan1 on user_holiday_plan1.user_id = user.id left join team_attendance.user_holiday_plan on user_holiday_plan.user_id = user_holiday_plan1.user_id and user_holiday_plan1.start_date = user_holiday_plan.start_date left join team_attendance.brand_holiday_plan on user_holiday_plan.brand_holiday_plan_id = brand_holiday_plan.id left join  (select user.id,sum(weight) as taken_days from user join brand_user on brand_user.user_id = user.id join team_attendance.absence_request on absence_request.user_id = user.id join team_attendance.absence_request_day on absence_request.id = absence_request_day.absence_request_id join team_attendance.brand_absence on absence_request.brand_absence_id = brand_absence.id where harri_brand_id in (select id from brand where group_id = 1466403) and is_primary=1 and is_scheduling_member = 1 and brand_absence.type = 'HOLIDAY' and absence_request.status = 'APPROVED' and weight =1 and team_attendance.absence_request.start_date > @final and absence_request.start_date < date(now()) group by user.id) as taken on user.id = taken.id left join (select user.id,sum(weight) as booked_days from user join brand_user on brand_user.user_id = user.id join team_attendance.absence_request on absence_request.user_id = user.id join team_attendance.absence_request_day on absence_request.id = absence_request_day.absence_request_id join team_attendance.brand_absence on absence_request.brand_absence_id = brand_absence.id where harri_brand_id in (select id from brand where group_id = 1466403) and is_primary=1 and is_scheduling_member = 1 and brand_absence.type = 'HOLIDAY'  and team_attendance.absence_request.start_date > date(now()) and absence_request.start_date < @eofy and absence_request.status= 'APPROVED' group by user.id) as booked_days on booked_days.id = user.id  where brand_user.is_primary=1 and is_scheduling_member = 1 and harri_brand_id in (select id from brand where group_id = 1466403) and user_holiday_plan.start_date >= @final and user_holiday_plan.deleted=0  order by harri_brand_id ; ";
            sql=" SELECT user.id, Concat(first_name, ' ', last_name) AS full_name, email, is_primary, `is_scheduling_member`, harri_brand_id, brand_holiday_plan.type, brand_holiday_plan.title AS holiday_plan_name, user_holiday_plan.start_date AS user_holiday_plan_start_date FROM user JOIN brand_user ON user.id = brand_user.user_id LEFT JOIN (SELECT id, user_id, accruals, brand_holiday_plan_id, Max(start_date) AS start_date, adjustments FROM team_attendance.user_holiday_plan WHERE deleted = 0 GROUP BY user_id) AS user_holiday_plan1 ON user_holiday_plan1.user_id = user.id LEFT JOIN team_attendance.user_holiday_plan ON user_holiday_plan.user_id = user_holiday_plan1.user_id AND user_holiday_plan1.start_date = user_holiday_plan.start_date LEFT JOIN team_attendance.brand_holiday_plan ON user_holiday_plan.brand_holiday_plan_id = brand_holiday_plan.id WHERE brand_user.is_primary = 1 AND is_scheduling_member = 1 AND harri_brand_id IN (SELECT id FROM brand WHERE group_id = @group_id) AND (user_holiday_plan.start_date >= @final or user_holiday_plan.start_date is null) AND (user_holiday_plan.deleted = 0 or user_holiday_plan.deleted is null) and (brand_holiday_plan.id in (select id from team_attendance.brand_holiday_plan where brand_id = @group_id) or brand_holiday_plan.id is null) ORDER BY harri_brand_id ;"
            row=cursor.execute(sql)
            for row in cursor:
                user_id=row['id']
                f.write(str(row['id']))
                f.write(',')
                f.write(str(row['full_name']))
                f.write(',')
                f.write(str(row['email']))
                f.write(',')
                f.write(str(row['harri_brand_id']))
                f.write(',')
                f.write(str(d[row['harri_brand_id']]))
                f.write(',')
                f.write(str(row['is_primary']))
                f.write(',')
                f.write(str(row['type'])) # brand holiday type
                f.write(',')
                f.write(str(row['holiday_plan_name']))
                f.write(',')
                f.write(str(row['user_holiday_plan_start_date']))
                f.write(',')
                if str(row['type']) != 'None':
                        url3 = "https://attendance-api.harri.com/api/v1/brands/"+ str(row['harri_brand_id']) +"/users/"+ str(user_id) +"/holiday_plans/stats"
                        print url3
                        r3 = requests.get(url3, headers=headers, cookies=Mycookie)
                        remaining = json.loads(r3.text)


                        remaining_annual_balance = remaining['data']['user_holiday_plan']['remaining_annual_balance']
                        adjustments = remaining['data']['user_holiday_plan']['adjustments']
                        accruals = remaining['data']['user_holiday_plan']['accruals']
                        booked = remaining['data']['user_holiday_plan']['booked']
                        taken = remaining['data']['user_holiday_plan']['taken']
                        allowance = remaining['data']['user_holiday_plan']['allowance']
                        available_to_date = remaining['data']['user_holiday_plan']['available_to_date']

                        f.write(str(round(accruals, 2)))
                        f.write(',')

                        f.write(str(round(adjustments, 2)))
                        f.write(',')

                        f.write(str(round(booked, 2)))
                        f.write(',')

                        f.write(str(round(taken, 2)))
                        f.write(',')

                        f.write(str(round(allowance, 2)))
                        f.write(',')

                        f.write(str(round(available_to_date, 2)))
                        f.write(',')

                        if str(row['type']) == 'FIXED':
                            full_annual_entitlement = remaining['data']['user_holiday_plan']['full_annual_entitlement']
                            f.write(str(round(full_annual_entitlement, 2)))

                        else:
                            f.write(' ')

                        f.write(',')
                        f.write(str(round(remaining_annual_balance, 2)))
                        f.write(',')

                        email=(str(row['email']))
                        d[user_id]=email
                        brand_id=(row['harri_brand_id'])
                        full_name=(row['full_name'])
                        holiday_type=(row['type'])
                        if holiday_type == 'FLEXIBLE' :
                            url2 = "https://lpm-aggregator.harri.com/api/v1/groups/brands/" + str(group_id) + "/users/" + str(user_id) + "/clocks/stats"
                            print url2
                            r2 = requests.get(url2, headers=headers, cookies=Mycookie)
                            holiday_data = json.loads(r2.text)
                            daily_paid = holiday_data['data']['stats']['daily_paid_amount'] / 100
                            daily_worked = holiday_data['data']['stats']['daily_worked_time'] / 3600
                            total_worked_days = holiday_data['data']['stats']['total_worked_days']

                            f.write(str(round(daily_paid,2)))
                            f.write(',')
                            f.write(str(total_worked_days))
                            f.write(',')
                            f.write(str(round(daily_worked,2)))
                            time.sleep(1)


                print user_id
                print "#############################"
                f.write("\n")



    if generate_pay_type_hourly_employee_report == True:
        f1 = open("/Users/imadkhuffash/Desktop/desktop/reports/" + str(d[group_id]) + " " + str(today) + " hourly employees.csv", "aw+")
        f1.truncate(0)
        f1.write('user_id,email,full name,harri_brand_id,is_primary,brand_name,pay type,pay type start_date,rate,pay rate start_date,position_name\r\n')
        sql="select distinct user_id, user.email, concat(user.first_name,' ', user.last_name) as name,is_primary, harri_brand_id,brand_user_pay_type.type,brand_user_pay_type.start_date as brand_user_pay_type_start_date,pay_rate.rate,pay_rate.start_date as pay_rate_start_date,position.name as position_name from brand_user left join `user` on user.id=brand_user.user_id join brand_user_position on brand_user_position.brand_user_id = brand_user.id left join position on position.id = brand_user_position.position_id left join (select brand_user_id,max(start_date) as start_date from brand_user_pay_type where deleted = 0 group by brand_user_id) as pay_type on pay_type.brand_user_id = brand_user.id left join brand_user_pay_type on brand_user_pay_type.brand_user_id = pay_type.brand_user_id and brand_user_pay_type.start_date = pay_type.`start_date` and brand_user_pay_type.deleted = 0 left join (select brand_user_position_id,max(start_date) as start_date from pay_rate where deleted =0 group by brand_user_position_id) as pay_rate1 on brand_user_position.id = pay_rate1.brand_user_position_id left join pay_rate on pay_rate1.brand_user_position_id = pay_rate.brand_user_position_id and pay_rate.start_date = pay_rate1.start_date where harri_brand_id in (select id from brand where group_id = @group_id)  and status = 'ACTIVE' and is_scheduling_member = 1 and (brand_user_position.deleted = 0 or brand_user_position.deleted is null) and brand_user.deleted = 0 and brand_user_pay_type.type = 'HOURLY';"
        with connection.cursor() as cursor2:
            cursor2.execute("set @group_id= " + str(group_id) + ";");
            row=cursor2.execute(sql)
            for row in cursor2:
                    user_id=row['user_id']
                    f1.write(str(row['user_id']))
                    f1.write(',')
                    f1.write(str(row['email']))
                    f1.write(',')
                    f1.write(str(row['name']))
                    f1.write(',')
                    f1.write(str(row['is_primary']))
                    f1.write(',')
                    f1.write(str(row['harri_brand_id']))
                    f1.write(',')
                    f1.write(str(d[row['harri_brand_id']]))
                    f1.write(',')
                    f1.write(str(row['type']))
                    f1.write(',')
                    f1.write(str(row['brand_user_pay_type_start_date']))  # brand holiday type
                    f1.write(',')
                    f1.write(str(row['rate']))
                    f1.write(',')
                    f1.write(str(row['pay_rate_start_date']))
                    f1.write(',')
                    f1.write(str(row['position_name']))
                    f1.write("\n")

                    print user_id




    if generate_pay_type_salaried_employee_report == True:
        f2 = open("/Users/imadkhuffash/Desktop/desktop/reports/" + str(d[group_id]) + " " + str(today) + " salaried employees.csv", "aw+")
        f2.truncate(0)
        f2.write('user_id,email,name,harri_brand_id,is_primary,brand_name,pay type,pay_type_start_date,annual salary,annual_salary_start_date,position name\r\n')
        sql="select distinct user_id, user.email, concat(user.first_name,' ', user.last_name) as name,is_primary, harri_brand_id,brand_user_pay_type.type,brand_user_pay_type.start_date as  'pay_type_start_date',brand_user_annual_rate.value/100 as 'annual_salary',brand_user_annual_rate.start_date as 'annual_salary_start_date',position.name as 'position_name' from brand_user left join user on user.id = user_id join brand_user_position on brand_user_position.brand_user_id = brand_user.id left join position on position.id = brand_user_position.position_id left join (select brand_user_id,max(start_date) as start_date from brand_user_pay_type where deleted = 0 group by brand_user_id) as pay_type on pay_type.brand_user_id = brand_user.id left join brand_user_pay_type on brand_user_pay_type.brand_user_id = pay_type.brand_user_id and brand_user_pay_type.start_date = pay_type.`start_date` and brand_user_pay_type.deleted =0 left join (select brand_user_id,max(start_date) as start_date from brand_user_annual_rate where deleted =0 group by brand_user_id) as annual_rate on brand_user.id = annual_rate.brand_user_id left join brand_user_annual_rate on brand_user_annual_rate.brand_user_id = annual_rate.brand_user_id and brand_user_annual_rate.start_date = annual_rate.start_date and brand_user_annual_rate.deleted = 0 where harri_brand_id in (select id from brand where group_id = @group_id)  and status = 'ACTIVE' and is_scheduling_member = 1 and (brand_user_position.deleted = 0 or brand_user_position.deleted is null) and brand_user.deleted = 0 and brand_user_pay_type.type in ('SALARIED','WEIGHTED_SHIFT');"
        with connection.cursor() as cursor3:
            cursor3.execute("set @group_id= " + str(group_id) + ";");
            row=cursor3.execute(sql)
            for row in cursor3:
                    user_id=row['user_id']
                    f2.write(str(row['user_id']))
                    f2.write(',')
                    f2.write(str(row['email']))
                    f2.write(',')
                    f2.write(str(row['name']))
                    f2.write(',')
                    f2.write(str(row['is_primary']))
                    f2.write(',')
                    f2.write(str(row['harri_brand_id']))
                    f2.write(',')
                    f2.write(str(d[row['harri_brand_id']]))
                    f2.write(',')
                    f2.write(str(row['type']))
                    f2.write(',')
                    f2.write(str(row['pay_type_start_date']))  # brand holiday type
                    f2.write(',')
                    f2.write(str(row['annual_salary']))
                    f2.write(',')
                    f2.write(str(row['annual_salary_start_date']))
                    f2.write(',')
                    f2.write(str(row['position_name']))
                    f2.write("\n")

                    print user_id


    if generate_LMS_report == True:
        try:
            connection = pymysql.connect(host='35.171.94.111',
                                         port=3311,
                                         user='nada.mahamdah',
                                         password='tPKxF?U4$H^NF^fN',
                                         db='lms',
                                         cursorclass=pymysql.cursors.DictCursor);

            f3 = open("/Users/imadkhuffash/Desktop/desktop/reports/" + str(today) + " LMS report.csv", "aw+")
            f3.truncate(0)
            f3.write('user_id,full name,email,last loging,brand_id,brand name,type,name,state,reg id,status,enrollment date,spent_time,completion time,expire_date,source\r\n')
            sql="select user_id,brand_id,course.name as course_name,registration.state as course_state,registration.id as 'reg_id',registration.status as 'registration_status' ,registration.created as 'enrollment_date',spent_time,registration_result.created as 'completion time',registration_result.expire_date,source from registration left join registration_result on registration.id = registration_result.registration_id left join course on course.id = registration.course_id;"
            with connection.cursor() as cursor4:
                cursor4.execute("set @group_id= " + str(group_id) + ";");
                row=cursor4.execute(sql)
                for row in cursor4:
                        user_id=str(row['user_id'])
                        f3.write(str(row['user_id']))
                        f3.write(',')
                        f3.write(',')
                        f3.write(',')
                        f3.write(',')
                        f3.write(str(row['brand_id']))
                        f3.write(',')
                        f3.write(',')
                        f3.write(',')
                        f3.write(str(row['course_name']))
                        f3.write(',')
                        f3.write(str(row['course_state']))
                        f3.write(',')
                        f3.write(str(row['reg _id']))
                        f3.write(',')
                        f3.write(str(row['registration_status']))
                        f3.write(',')
                        f3.write(str(row['enrollment_date']))
                        f3.write(',')
                        f3.write(str(row['spent_time']))  # brand holiday type
                        f3.write(',')
                        f3.write(str(row['completion time']))
                        f3.write(',')
                        f3.write(str(row['expire_date']))
                        f3.write(',')
                        f3.write(str(row['source']))
                        f3.write("\n")
                        print user_id
        except Exception as e:
            print e.message, e.args





except Exception as e:
    print e.message, e.args
print("done")