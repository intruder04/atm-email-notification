#mayorov 2016
#рассылка писем о новых инцидентах на всп + рассылка о закрывшихся
#используется база sq_lite (db/vsp_us_IDs.sqlite) для хранения уже отосланных инцидентов и модуль cx_oracle для подключения к бд Oracle Service Manager IT
import smtplib, time, cx_Oracle, datetime, sqlite3, re, threading, os
from email.mime.text import MIMEText
from pprint import pprint

#time delta params
days = 0
new_seconds = 600 #10 minutes
done_seconds = 600

#query for SMIT
query_text = "select p.\"NUMBER\" as ID, p.hpc_open_time as open_time, p.hpc_next_breach as srok, p.brief_description as tema, p.action as inf, p.hpc_assignment_name as grp, p.hpc_logical_name_name as ci, dev.tps_search_code as atmname, nvl(dev.tps_coordinator_name,'0') as vsp, dev.tps_atm_address as adress from smprimary.probsummarym1 p left join smprimary.device2m1 dev on dev.logical_name=p.logical_name where p.hpc_status = 'qqqSTATUSqqq' and p.hpc_open_time>to_date('qqqTIMEqqq','DD.MM.YYYY HH24:MI:SS')-3.0/24 and (p.hpc_contact_name_name='user' or p.hpc_dept_name='dept') and dev.sb_is_vsp='t'"

#get time delta
time_actual = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
time_delta_new = (datetime.datetime.now() - datetime.timedelta(days,new_seconds)).strftime("%d.%m.%Y %H:%M:%S")
time_delta_done = (datetime.datetime.now() - datetime.timedelta(days,done_seconds)).strftime("%d.%m.%Y %H:%M:%S")

#get emails list
with open("emails.txt") as f:
	email_lines = f.read().splitlines()
f.close()

#get 9042 emails dictionary from file (can't generate 9042 emails)
with open("9042_emails.txt") as t:
	email_lines_9042 = dict(x.rstrip().split(";") for x in t)
t.close()

#mail func
def sendMail(email,text,subj):
	msg = MIMEText(text)
	msg['Subject'] = subj
	msg['From'] = "Mon_IT_serv@nnov.sbrf.ru"
	msg['To'] = email
	# msg['Bcc'] = "0042-mayorov-vv@nnov.sbrf.ru"
	msg['Content-Type'] = "text/html; charset=utf-8"
	# s = smtplib.SMTP('0042-s-es-04')
	s = smtplib.SMTP('10.222.180.53')
	s.ehlo()
	s.starttls()
	s.ehlo
	s.login('mon_it_serv','********')
	s.send_message(msg)
	s.quit()

#func to generate email or to compare SMIT gosb+vsp to saved emails from file
#8611 and 8614 need attention cause emails have no template
#9042 email cannot be genereated at all so there's 9042_emails.txt to define email explicitly
def define_email_for_vsp(vsp_string):
	if vsp_string != "0":
		vsp = re.sub(r'\(.*?\)','',vsp_string)
		vsp = (re.sub(r'ВВБ ','',vsp)).strip()
		vsp = vsp.split('/')
		global gosb_num
		global vsp_num
		gosb_num = vsp[0]
		vsp_num = (vsp[1])[-4:]

		global email
		if gosb_num=="8589": email = gosb_num+"-"+vsp_num+"@nnov.sbrf.ru"
		elif gosb_num=="8610": email = gosb_num+"-"+vsp_num+"-POST@nnov.sbrf.ru"
		elif gosb_num=="8611":
			vsp_num_tmp = vsp_num[-3:]
			# if vsp_num.startswith("000"):
				# vsp_num = vsp_num[-3:]
			# elif vsp_num.startswith("00"):
				# vsp_num_tmp = vsp_num[-3:]
			if ((gosb_num+"-"+vsp_num+"-") in email_lines):	email = gosb_num+"-"+vsp_num+"-@nnov.sbrf.ru"
			elif ((gosb_num+"-"+vsp_num_tmp+"-") in email_lines): email = gosb_num+"-"+vsp_num_tmp+"-@nnov.sbrf.ru"
		elif gosb_num=="8612": email = gosb_num+"-mbx-f"+vsp_num+"@nnov.sbrf.ru"
		elif gosb_num=="8613": email = gosb_num+"-"+vsp_num+"-uni@nnov.sbrf.ru"
		elif gosb_num=="8614":
			#исключение по просьбе лебедевой наталии вячеславовны (начальник ЦОПП 8614/7770, email - 8614-7770-ЦОПП-Общий <8614-0011-Obshiy@nnov.sbrf.ru>)
			if gosb_num=="8614" and vsp_num=="0011": 
				print("ignoring ",gosb_num,vsp_num)
				return 0
			# email = gosb_num+"-"+vsp_num+"-Obshiy@nnov.sbrf.ru, "+gosb_num+"-mbx-"+vsp_num+"-Obshiy@nnov.sbrf.ru"
			if ((gosb_num+"-"+vsp_num+"-Obshiy") in email_lines): email = gosb_num+"-"+vsp_num+"-Obshiy@nnov.sbrf.ru"
			else: email = gosb_num+"-mbx-"+vsp_num+"-Obshiy@nnov.sbrf.ru"
		elif gosb_num=="9042": email = email_lines_9042.get(gosb_num+"-"+vsp_num)
		else:
			print("no email found for gosb",gosb_num,vsp_num)
			return 0
		return email
	else:
		return 0
	
#main func
def ProcessData(status,time,query):
	#insert time+status params in query text
	query = (query.replace("qqqTIMEqqq",time)).replace("qqqSTATUSqqq",status)
	if status == "6 Закрыт":
		query = query.replace("hpc_open_time","HPC_ACTUAL_FINISH")
		sqlite_table_name = "done_calls"
	else:
		sqlite_table_name = "new_calls"
	con = cx_Oracle.connect('user/pass@10.67.18.222:1523/smrep')
	cur = con.cursor()
	cur.execute(query)
	res = cur.fetchall()

	for tuple in res:
		id = tuple[0]
		srok = str(tuple[2])
		tema = tuple[3]
		#using read() to get LOB object and convert it to string
		inf = tuple[4].read()
		group = tuple[5]
		ci = tuple[6]
		vsp = tuple[8]
		adress = tuple[9]

		text = '''Добрый день!<br>
На УС № '''+ci+''' расположенному по адресу '''+adress+''' зафиксирована проблема: '''+tema+'''<br>
Информация из запроса на проведение работ: '''+inf+'''<br>
Назначен исполнитель: '''+group+'''<br>
Для решения данного отклонения установлен контрольный срок: '''+srok+'''<br>
Номер инцидента: '''+id+'''<br>
<br>
-----------------<br>
Внимание! Данное сообщение создано автоматически. <br>
Просьба не отвечать на данное сообщение!<br>'''
		#do nothing if id is already in sqlite db
		if sqlite_db(id,"check",sqlite_table_name,time_actual,ci) == 0:
			if sqlite_table_name == "new_calls":
				subj = "СОЗДАН ИНЦИДЕНТ по УС № "+ci+". Информация о проведении работ"
			else:
				subj = "ВЫПОЛНЕН ИНЦИДЕНТ по УС № "+ci+". Информация о проведении работ"
			if define_email_for_vsp(vsp) != 0:
				email = define_email_for_vsp(vsp)
				print("	sending mail to",email, id)
				
				sendMail(email,text,subj)
				
				# insert sent ID to sqlite db
				sqlite_db(id,"insert",sqlite_table_name,time_actual,ci)
			else:
				print("vsp equals '0' in SMIT DB. Ignoring...")
	cur.close()
	con.close()

#func for sqlite db IDs storage. Selections/insertions to avoid email duplication
def sqlite_db(call_id, operator, table, time, ci):
	conn = sqlite3.connect('C:\\ov_scripts\\vsp_email_atm\\db\\vsp_us_IDs.sqlite')
	c = conn.cursor()
	if operator == "check":
		c.execute ("""select sum(case when id = '"""+call_id+"""' then 1 else 0 end) from """+table+"""""")
		for row in c:
			# print(row)
			if row[0]>0:
				print(table,call_id,"- checking if it was sent before...already sent!")
				return 1
			else:
				print(table,call_id,"- checking if it was sent before...it wasn't!")
				return 0
	elif operator == "insert":
		c.execute("""INSERT INTO """+table+"""(id,time,gosb,vsp,email,ci) VALUES (?,?,?,?,?,?)""", (call_id,time,gosb_num,vsp_num,email,ci,))
		conn.commit()		
	c.close()

if os.path.exists("log.txt"):	
	if os.path.getsize("log.txt") > 30000000:
		try:
			os.remove("log.txt")
		except OSError:
			pass
	

#get it started. threads for speed
print("========START========= ",time_actual,"\n")
t = threading.Thread(target=ProcessData, args=("2 Назначен",time_delta_new,query_text))
t1 = threading.Thread(target=ProcessData, args=("6 Закрыт",time_delta_done,query_text))
t.start()
t1.start()
t.join()
t1.join()
print("========FINISH======== ")
