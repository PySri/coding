#! /usr/bin/env python

import csv
import re
from simple_imdb import *

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


    # Happy path testing for Scale table queries
  
    # Test > predicate 
    print "Calling query Scale" 
    ag = 'COUNT'
    result,ret_list,aggr = Query('Scale',['C1', 'C2>1000000'],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

    # Test "=" predicate
    print "Calling query Scale" 
    ag = 'COUNT'
    result,ret_list,aggr = Query('Scale',['C1', 'C3=393317910946089938'],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

    # Test "<" predicate
    print "Calling query Scale" 
    ag = 'COUNT'
    result,ret_list,aggr = Query('Scale',['C50', 'C5<5987865435138336340'],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

    # Test empty predicate
    print "Calling query Scale" 
    ag = 'COUNT'
    result,ret_list,aggr = Query('Scale',['C25', ''],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

    # Test empty predicate with SUM aggr
    print "Calling query Scale" 
    ag = 'SUM'
    result,ret_list,aggr = Query('Scale',['C40', ''],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

    # Test expression with empty predicate
    #print "Calling query Scale" 
    ag = 'SUM'
    result,ret_list,aggr = Query('Scale',['C25+C34', ''],ag)
    if result:
        print "Query is successful : "
        print "Resulting table is : ", ret_list
        print "Aggregate ", ag, ' ', aggr
    else:
        print "Query returned Error"

    tablenames = ['Scale']
    agl = ['SUM','COUNT','NONE']
    predl = ['C75>4876000000000000000', 'C60<7230000000000000000', 'C3=8527285376860882071', '']
    exprl =['C39', 'C45+43', 'C1+C100']

if __name__ == '__main__':
    main()
