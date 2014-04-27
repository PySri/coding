#! /usr/bin/env python

import csv
import re

''' Simple In Memory Database program.
    Schema has the format : columnname1(STRING|INTEGER);columnname2(STRING|INTEGER);...;columnnamen(STRING|INTEGER)
    Tablename is a string
    CreateTable takes two arguments: tablename and Schema
    LoadTableFromCsv takes two arguments: tablename and name of the csv file
    Query takes three arguments: tablename, query structure, aggregation.
    query structure format : [(<expression),(predicate)]
    aggregation: SUM|COUNT|NONE'''

db = {}
table_datatype = {}

def createTable(tname, sch):
    '''CreateTable takes two arguments: tablename and Schema
       Schema has the format : columnname1(STRING|INTEGER);columnname2(STRING|INTEGER);...;columnnamen(STRING|INTEGER)'''

    pat = r'([\w ]+)\(([\w ]+)\)'
    parsed_data = re.findall(pat,sch)

    templ = []
    for x,y in parsed_data:
        table_datatype[(tname,x)] = y
        templ.append(x)

    db[tname] = [tuple(templ)]
    #db[tname] = [tuple(sch)]

def LoadTableFromCsv(tablename, csvfile):
    '''LoadTableFromCsv takes two arguments: tablename and name of the csv file'''

    with open(csvfile,'Ur') as f:
        data = list(tuple(rec) for rec in csv.reader(f,delimiter=','))

    record_label =  db[tablename][0]

    if record_label != data[0]:
       print "Table : ", tablename, "schema does NOT match"
       #print "Expected : ", record_label
       #print "Actual   : ", data[0]
       cflag = False
    else:
       #print "Schema matched for ", tablename
       #print "Expected : ", record_label
       #print "Actual   : ", data[0]
       cflag = True

    if cflag:
       i = 0
       #print "data 0 is : ", data[0]
       for entry in data:
          #print "loadtable: Entry of the record: ", entry

          if i > 0:
             x = 0
             templ = []
             for item in entry:
                 #get data type for the item. convert to integer if needed.

                 if table_datatype[(tablename,data[0][x])] == "INTEGER":
                     templ.append(int(item))
                 else:
                     templ.append(item)
                 x += 1

             if len(entry) == len(record_label) and i > 0:
                 #print "Entry is : ", entry
                 db[tablename].append(tuple(templ))
 
          i += 1
    else:
       print "Loading table from CSV file encountered error"

def Query(tablename, qstruct, aggregate = "NONE"):

   '''Query takes three arguments: tablename, query structure, aggregation.
   query structure format : [(<expression),(predicate)]
   aggregation: SUM|COUNT|NONE'''

   #print "Table to work on : ", tablename
   filtered_list = []

   # Query structure is two member list
   if len(qstruct) != 2:
      print "Query structure does not have two parts (expression and predicate)"
      return [(False,[],'None')]

   if db.has_key(tablename) == False:
      print "Table : ", tablename, " NOT FOUND in database"
      return (False,[],'None')

   expression_str = qstruct[0]
   predicate_str = qstruct[1]

   #print "Expression String is: ", expression_str
   #print "predicate String is: ", predicate_str

   pred_pat1 = r'([\w +]+)(>)([\w ]+)'
   pred_pat2 = r'([\w +]+)(<)([\w ]+)'
   pred_pat3 = r'([\w +]+)(=)([\w ]+)'
   expr_pat  = r'([\w ]+)'

   # Parse and match predicate string
   p_match = []
   if predicate_str != "":
       p_match = re.findall(pred_pat1, predicate_str)
       p_check = "greater"
       if p_match == []:
          p_match = re.findall(pred_pat2, predicate_str)
          p_check = "lesser"
       if p_match == []:
          p_match = re.findall(pred_pat3, predicate_str)
          p_check = "equals"
       if p_match == []:
           print "Unsupported predicate string", predicate_str
           return (False,[],'None')
   else:
       p_check = "None"

   # Parse and match expression string
   e_match = re.findall(expr_pat,expression_str)

   if e_match == []:
       print "Unable to parse expression: ", expression_str
       return (False,[],'None')
   #else:
       #print "Expression parsed : ", e_match

   for word in e_match:
       if word not in db[tablename][0]:
           print "Column : ", word, "Does not exist in table: ", tablename
           return (False,[],'None')

   # Parse the expression part of the predicate string
   index_list = []
   if p_check != 'None':
       expr = p_match[0][0]
       filtered_list = []
       pred_expr_match = re.findall(expr_pat,expr)
       prev_data_type = table_datatype[(tablename,pred_expr_match[0])]

       for w in pred_expr_match:
           if table_datatype[(tablename,w)] != prev_data_type :
                 print "Inter-mix of data types are not allowed."
                 return (False,[],'None')
           idx = db[tablename][0].index(w)
           index_list.append(idx)

       # Traverse the records and apply the predicate rule
       x = 0
       for e in db[tablename]:
           p_match_flag = False
           if x != 0:
              templ = []
              for i in index_list:
                  #print db[tablename][x][i]
                  templ.append(db[tablename][x][i])
              #print "working list: ", templ

              if prev_data_type == 'INTEGER':
                  i_pred_expr_net_result = 0

                  for item in templ:
                      i_pred_expr_net_result += item

                  if p_check == 'equals':
                      if i_pred_expr_net_result == int(p_match[0][2]):
                             #print "Equals: Found predicate match for the the record: ", e
                             p_match_flag = True
                  elif p_check == 'greater':
                        if i_pred_expr_net_result > int(p_match[0][2]):
                             #print "Greater: Found predicate match for the record: ", e, i_pred_expr_net_result
                             p_match_flag = True
                  elif p_check == 'lesser':
                        if i_pred_expr_net_result < int(p_match[0][2]):
                             #print "Lesser: Found predicate match for the record: ", e
                             p_match_flag = True
              elif prev_data_type == 'STRING':
                  s_pred_expr_net_result = ''

                  for item in templ:
                      s_pred_expr_net_result += item

                  if p_check == 'equals':
                      if s_pred_expr_net_result == p_match[0][2]:
                             #print "Equals: Found predicate match for the the record: ", e
                             p_match_flag = True
                  elif p_check == 'greater':
                        if s_pred_expr_net_result > p_match[0][2]:
                             #print "Greater: Found predicate match for the record: ", e, s_pred_expr_net_result
                             p_match_flag = True
                  elif p_check == 'lesser':
                        if s_pred_expr_net_result < p_match[0][2]:
                             #print "Lesser: Found predicate match for the record: ", e
                             p_match_flag = True

              #If predicate match found append the record
              if p_match_flag:
                  filtered_list.append(e)
           x += 1

   #print "Filtered one is : ", filtered_list

   #Apply expression on filtered list or actual list

   if p_check == 'None':
      # Remove the header and add the reminder of the list
      working_list = db[tablename][1::]
   elif p_check != 'None' and filtered_list != []:
       working_list = filtered_list
   else:
       print "Predicate query matched no records"
       return (False,[],'None')

   #print "Working list is :", working_list

   prev_data_type = table_datatype[(tablename,e_match[0])]
   #print "data type to work on is ", prev_data_type

   index_list = []
   for w in e_match:
       if table_datatype[(tablename,w)] != prev_data_type :
           print "Inter-mix of data types are not allowed."
           return (False,[],'None')

       idx = db[tablename][0].index(w)
       index_list.append(idx)

   # traverse the working list and apply the expression
   result_list = []
   #print "index list is : ", index_list
   #print "expression list is ", e_match

   #print "working list is: ", working_list

   for e in working_list:
       if prev_data_type == "INTEGER":
           temp_x = 0
           for i in index_list:
               #print 'e of i is ', e[i]
               v = int(e[i])
               #print 'v is ', v
               temp_x += v
       elif prev_data_type == "STRING":
           temp_x = ''
           for i in index_list:
               temp_x += e[i]

       result_list.append(temp_x)

   print "Resulting list is : ", result_list

   # Apply aggregation if needed.
   # Validate aggregate keyword
   if aggregate == "SUM":
       print "Aggregation needs to do SUM"
       if type(result_list[0]) is int or type(result_list[0]) is long:
           aggr_result = sum(result_list)
           #print "SUM is: ", aggr_result
           #print "Resulting list is: ", result_list
   elif aggregate == "COUNT":
       print "Aggregation needs to do COUNT"
       aggr_result = len(result_list)
       #print "COUNT is: ", aggr_result
       #print "Resulting list is: ", result_list
   elif aggregate == "NONE":
       print "No need to aggregate"
       print "Resulting list is: ", result_list
       aggr_result = 0
   else:
       print "Unsupported aggregation type (Only COUNT/SUM supported)"
       return (False,[],'None')

   return (True,result_list,aggr_result)

def main():
    createTable('empl','name(STRING);age(INTEGER);gender(STRING)')
    createTable('Student', 'name(STRING);grade(INTEGER)')
    createTable('inb', 'junk1(STRING);junk2(STRING)')
    createTable('Scale','C1(INTEGER);C2(INTEGER);C3(INTEGER);C4(INTEGER);C5(INTEGER);C6(INTEGER);C7(INTEGER);C8(INTEGER);C9(INTEGER);C10(INTEGER);C11(INTEGER);C12(INTEGER);C13(INTEGER);C14(INTEGER);C15(INTEGER);C16(INTEGER);C17(INTEGER);C18(INTEGER);C19(INTEGER);C20(INTEGER);C21(INTEGER);C22(INTEGER);C23(INTEGER);C24(INTEGER);C25(INTEGER);C26(INTEGER);C27(INTEGER);C28(INTEGER);C29(INTEGER);C30(INTEGER);C31(INTEGER);C32(INTEGER);C33(INTEGER);C34(INTEGER);C35(INTEGER);C36(INTEGER);C37(INTEGER);C38(INTEGER);C39(INTEGER);C40(INTEGER);C41(INTEGER);C42(INTEGER);C43(INTEGER);C44(INTEGER);C45(INTEGER);C46(INTEGER);C47(INTEGER);C48(INTEGER);C49(INTEGER);C50(INTEGER);C51(INTEGER);C52(INTEGER);C53(INTEGER);C54(INTEGER);C55(INTEGER);C56(INTEGER);C57(INTEGER);C58(INTEGER);C59(INTEGER);C60(INTEGER);C61(INTEGER);C62(INTEGER);C63(INTEGER);C64(INTEGER);C65(INTEGER);C66(INTEGER);C67(INTEGER);C68(INTEGER);C69(INTEGER);C70(INTEGER);C71(INTEGER);C72(INTEGER);C73(INTEGER);C74(INTEGER);C75(INTEGER);C76(INTEGER);C77(INTEGER);C78(INTEGER);C79(INTEGER);C80(INTEGER);C81(INTEGER);C82(INTEGER);C83(INTEGER);C84(INTEGER);C85(INTEGER);C86(INTEGER);C87(INTEGER);C88(INTEGER);C89(INTEGER);C90(INTEGER);C91(INTEGER);C92(INTEGER);C93(INTEGER);C94(INTEGER);C95(INTEGER);C96(INTEGER);C97(INTEGER);C98(INTEGER);C99(INTEGER);C100(INTEGER)')

    #print 'value of db is: ', db
    #print "Enumberated list: ", ed
    #for i,n in ed:
       #print "index is :", i, " Value is :", n
       #print "Schema for ", n, "is: ", db[n]

    LoadTableFromCsv("empl","b1.csv")
    LoadTableFromCsv('Student', 'b2.csv')
    LoadTableFromCsv('Scale','testfile.csv')

    #for ke in db.keys():
       #if len(db[ke]) > 1:
          #print "Table name : ", ke, " Values: ", db[ke]

    #Query('empl',["name+age","age=30"],"NONE")
    #Query('empl',['name+gender', 'age+age>100'],'COUNT')
    ag = 'SUM'
    result, ret_list,aggr = Query('empl',['age', 'age+age>100'],ag)
    if result:
        print "Query is successful : " 
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

    ag = 'COUNT'
    result, ret_list,aggr = Query('empl',['age', 'age+age>100'],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"
    #Query('empl',['age+gender', ''])
    #Query('empl',['name+gender', 'age<29'])
    #Query('empl',['name+age','name=Srini'])

   
    print "Calling query Scale" 
    ag = 'COUNT'
    result,ret_list,aggr = Query('Scale',['C1', 'C2>1000000'],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

if __name__ == '__main__':
    main()
