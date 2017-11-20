import pandas as pd
import numpy as np
import time
import sys
import re

notMap = {
    '==' : '!=',
    '!=' : '==',
    '>' : '<=',
    '<' : '>=',
    '>=' : '<',
    '<=' : '>',
    '|' : '&',
    '&' : '|'
}


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

def checkStackEnd(stk):
    while len(stk) > 0:
        if stk[-1] in ["&", "|", "~"]:
            stk.pop()
        else:
            break

def conditionToPandas(condition, Final = False):
    assert len(condition) == 3
    expression = '('
    if condition[0][0] == '(':
        expression += condition[0]
    elif condition[2] in isAttribute or condition[2][0] == '(' or Final:
        expression += 'df_result[\'' + condition[0] + '\']'
    else:
        for i in range(len(tableprefixList)):
            if condition[0] in attrDict[tableprefixList[i]]:
                expression += 'dataDict[\'' + tableprefixList[i] + '\'][\'' + condition[0] + '\']'

    expression += condition[1]

    if is_number(condition[2]) or condition[1] == '.str.match(' or condition[2][0] == '(':
        expression += condition[2]
    elif condition[2] in isAttribute:
        expression += 'df_result[\'' + condition[2] + '\']'
    else:
        expression += '\'' + condition[2] + '\''
    expression += ')'

    return expression

def checkNotCondition(condition):
    index = 0
    while index < len(condition):
        con = condition[index]
        if con == '~':
            if index == len(condition)-1:
                del condition[index]
                break
            if condition[index + 1] == "(":
                pos = index
                while condition[pos] != ')':
                    if condition[pos] in notMap:
                        condition[pos] = notMap[condition[pos]]
                    pos += 1
            else:
                condition[index + 2] = notMap[condition[index + 2]]
            del condition[index] #delete not

        index += 1


def  handleCondition(condition):
    for i in range(len(condition)):
        con = condition[i]
        if con == '=':
            condition[i] = '=='
        elif con == '<>':
            condition[i] = '!='
        elif con == 'or' or con == 'OR':
            condition[i] = '|'
        elif con == 'and' or con == 'AND':
            condition[i] = '&'
        elif con == 'not' or con == 'NOT':
            condition[i] = '~'
        elif con == 'like' or con == 'LIKE':
            condition[i] = '.str.match('
            #if condition[i+1][1] != '%':
            condition[i + 1] = '\'^' + condition[i + 1][1:]
            #if condition[i + 1][len(condition[i + 1])-2] != '%':
            condition[i + 1] = condition[i + 1][:len(condition[i + 1])-1] + '$\', na=False)'
            condition[i + 1] = condition[i + 1].replace('%', '.*')
            condition[i + 1] = condition[i + 1].replace('_', '.')


while True:
    df_result = pd.DataFrame({'A' : []})
    command = input("Type the your SQL and press Enter: \n>")
    start_time = time.time()
    #command = 'SELECT title_year,movie_title,Award,imdb_score FROM movies.csv M, oscars.csv A WHERE M.movie_title = A.Film AND M.imdb_score < 7'
    if len(command) < 1:  # check prevents a crash when indexing to 1st character
       continue
    sqlList = command.split() #Create the list to store the SQL
    colList = sqlList[1].split(',')
    attributeList = list(colList)
    # print(colList)
    dataDict = dict()
    attrDict = dict()
    joinCon = []

    conPos = -1
    if 'WHERE' in sqlList:
        conPos = sqlList.index('WHERE') + 1  # conPos = the position conditional command starts
    if 'where' in sqlList:
        conPos = sqlList.index('where') + 1
    if conPos != -1:
        for c in sqlList[conPos:]:
            attributeList.append(c)
    # print(attributeList)
    #FROM: Get the outer join of several tables or the content of one single file
    fileList = [] #Retrieve one or more filename
    if conPos == -1:
        tableList = sqlList[3:]
    else:
        tableList = sqlList[3:conPos - 1]
    tableList1 = list(tableList)
    tableposList = []
    for i in range(len(tableList)):
        if '.csv' in tableList[i]:
            tableList[i] = tableList[i].replace('.csv,','.csv')
            tableposList.append(i)
            fileList.append(tableList[i])
            tableList[i] = tableList[i].replace('.csv','.')
        else:
            if ',' in tableList[i]:
                tableList[i] = tableList[i].replace(',','.')
            else:
                tableList[i] += '.'
    #tableposList.append(conPos-4) #append the position of WHERE as the last value in list

    #find the tables needed to be renamed.
    #print(tableList1)
    #TODO: only support inner join now
    joinPos = -1
    if 'JOIN' in tableList1:
        joinPos = tableList1.index('JOIN')
    elif 'join' in tableList1:
        joinPos = tableList1.index('join')

    renametableList = [] #Save the tables needed to be renamed
    tableprefixList = [] #Save the prefix of each table
    if joinPos != -1:
        if tableList1[1].upper() != "JOIN":
            renametableList.append(tableList[0])
        #find all position of JOIN
        jlist = [index for index, value in enumerate(tableList1) if value.upper() == 'JOIN']
        #find all position of ON
        olist = [index for index, value in enumerate(tableList1) if value.upper() == 'ON']
        if len(jlist) != len(olist):
            sys.exit("illegal command")
        for k in range(len(jlist)):
            if olist[k] - jlist[k] == 3:
                renametableList.append(tableList[jlist[k]+1])
            #joinCon List
            idx = olist[k] + 1 #start of join expression
            if idx + 2 < len(tableList1):
                t = tableList1[idx : idx + 3]
                handleCondition(t)
                joinCon.append("".join(t))
            else:
                sys.exit("illegal command")
    else:
        if len(tableList) > 1:
            renametableList.append(tableList[0])
    """
    for i in range(len(tableposList)):
        if i > 0 and tableposList[i]-tableposList[i-1] == 2:
            renametableList.append(tableList[tableposList[i-1]])
    """
    #Save the prefix of each table(renamed or not)
    j = 0
    for i in range(len(tableposList)):

        if j<len(renametableList) and tableList[tableposList[i]] == renametableList[j]:
            tableprefixList.append(tableList[tableposList[i]+1])
            j = j + 1
        else:
            tableprefixList.append(tableList[tableposList[i]])

    print(df_result.columns)
    print('tableList',tableList)
    print('tableposList',tableposList)
    print('renametableList',renametableList)
    print('tableprefixList',tableprefixList)
    print('fileList',fileList)
    print('joinCon: ', joinCon)
    isAttribute = []

    if len(fileList) == 1:
        df_result = pd.read_csv(fileList[0]) #, assume_missing=True)#, keep_default_na=False)
        isAttribute = set(df_result.columns)
        attrDict[tableprefixList[0]] = set(list(df_result.columns))
        dataDict[tableprefixList[0]] = df_result

    else:
        for i in range(len(fileList)):
            df = pd.read_csv(fileList[i]) #, assume_missing=True)
            if colList != ['*']:
                required_columns = set()
                cols = df.columns
                for attr in attributeList:
                    if attr in cols:
                        required_columns.add(attr)
                        isAttribute.append(attr)
                    if '.' in attr and attr.split('.')[1] in cols:
                        required_columns.add(attr.split('.')[1])
                        isAttribute.append(attr)
                    elif '+' in attr and attr.split('+')[0] in cols:
                        required_columns.add(attr.split('+')[0])
                        isAttribute.append(attr)
                    elif '+' in attr and attr.split('+')[1] in cols:
                        required_columns.add(attr.split('+')[1])
                        isAttribute.append(attr)
                    elif '-' in attr and attr.split('-')[0] in cols:
                        required_columns.add(attr.split('-')[0])
                        isAttribute.append(attr)
                    elif '-' in attr and attr.split('-')[1] in cols:
                        required_columns.add(attr.split('-')[1])
                        isAttribute.append(attr)
                    elif '*' in attr and attr.split('*')[0] in cols:
                        required_columns.add(attr.split('*')[0])
                        isAttribute.append(attr)
                    elif '*' in attr and attr.split('*')[1] in cols:
                        required_columns.add(attr.split('*')[1])
                        isAttribute.append(attr)
                    elif '/' in attr and attr.split('/')[0] in cols:
                        required_columns.add(attr.split('/')[0])
                        isAttribute.append(attr)
                    elif '/' in attr and attr.split('/')[1] in cols:
                        required_columns.add(attr.split('/')[1])
                        isAttribute.append(attr)
                # print(required_columns)
                df = df[list(required_columns)]
            if len(renametableList) > 0:
                cols = list(df.columns)
                for j in range(len(cols)):
                    cols[j] = tableprefixList[i] + cols[j]
                df.columns = cols
            attrDict[tableprefixList[i]] = set(list(df.columns))
            dataDict[tableprefixList[i]] = df
            isAttribute.extend(list(df.columns))

    print('isAttribute', isAttribute)
    print(list(df_result.columns))

    #WHERE: Deal with the conditions
    stk = []
    condRes = []
    IsOr = False
    conList = []
    lastOper = ""
    if conPos != -1:
        condition = sqlList[conPos:]
        handleCondition(condition)
        checkNotCondition(condition)
        #print(condition)
        if "|" in condition:
            IsOr = True
        for i in range(len(condition)):
            con = condition[i]
            if con == '|':
                if len(conList) == 3: #TODO also check attr
                    expr = conditionToPandas(conList, True)
                    if lastOper != "":
                        condRes.append(lastOper)
                    condRes.append(expr)
                    lastOper = "|"
                    conList.clear()
                else:
                    lastOper = "|"
                    if i == 0 or condition[i-1] != ')':
                        sys.exit("illegal command")
            elif con == '&':
                if len(conList) == 3: #TODO also check attr
                    if not IsOr and (conList[0] in isAttribute and conList[2] not in isAttribute):
                        expr = conditionToPandas(conList)
                        print("<<filter>>", expr)
                        for t in tableprefixList:
                            if conList[0] in attrDict[t]:
                                dataDict[t] = dataDict[t][eval(expr)]
                                break
                    else:
                        expr = conditionToPandas(conList, True)
                        if lastOper != "":
                            condRes.append(lastOper)
                        condRes.append(expr)
                        lastOper = "&"
                    conList.clear()
                else:
                    lastOper = "&"
                    if i == 0 or condition[i-1] != ')':
                        sys.exit("illegal command")
            elif con == '(':
                stk.append(list(condRes))
                stk.append(lastOper)
                condRes.clear()
                conList.clear()
                lastOper = ""
            elif con == ')':
                if len(conList) == 3:
                    if not IsOr and (conList[0] in isAttribute and conList[2] not in isAttribute):
                        expr = conditionToPandas(conList)
                        print("<<filter>>", expr)
                        for t in tableprefixList:
                            if conList[0] in attrDict[t]:
                                dataDict[t] = dataDict[t][eval(expr)]
                                break
                    else:
                        expr = conditionToPandas(conList, True)
                        if lastOper != "":
                            condRes.append(lastOper)
                        condRes.append(expr)
                lastOper = ""
                conList.clear()
                #print(stk)
                o = stk.pop()
                lastCond = stk.pop()
                if len(condRes) != 0:
                    lastCond.append(o)
                    lastCond.append('(')
                    lastCond.extend(condRes)
                    lastCond.append(')')
                condRes = lastCond
            else:
                conList.append(con)
        #end of command
        if len(conList) == 3: #TODO also check attr
            if not IsOr and (conList[0] in isAttribute and conList[2] not in isAttribute):
                expr = conditionToPandas(conList)
                print("<<filter>>", expr)
                for t in tableprefixList:
                    if conList[0] in attrDict[t]:
                        dataDict[t] = dataDict[t][eval(expr)]
                        break
            else:
                expr = conditionToPandas(conList, True)
                if lastOper != "":
                    condRes.append(lastOper)
                condRes.append(expr)
                lastOper = ""
            conList.clear()

        cond_str = " ".join(condRes)
        print("[[Query]]: ", cond_str)

    ## JOIN
    #print(dataDict.values())
    if len(joinCon) != 0:
        print(joinCon)
        [left, right] = joinCon[0].split('==')
        for select in colList:
            if select in right or right in select:
                temp = right
                right = left
                left = temp
        for key, value in attrDict.items():
            if right in value:
                coll = list(dataDict[key].columns)
                for i in range(len(coll)):
                    if coll[i] == right:
                        coll[i] = left
                        break
                dataDict[key].columns = coll
        for df in dataDict.values():
            #df = (df).assign(key=0)
            print(df.columns)
            print("--- %s seconds ---" % (time.time() - start_time))
            try:
                df_result = df_result.merge(df, how='inner', on=left)
                # df_result.drop('key', 1, inplace=True)
            except:
                df_result = df

    else:
        for df in dataDict.values():
            df = (df).assign(key=0)
            print("--- %s seconds ---" % (time.time() - start_time))
            try:
                df_result = df_result.merge(df, how='outer', on='key')
                # df_result.drop('key', 1, inplace=True)
            except:
                df_result = df

    if cond_str != "":
        df_result = df_result[eval(cond_str)]

    #SELECT: Get the column(attributes) from file
    if colList == ['*']:
        print(df_result)
    else:
        result_attr = []
        columns = df_result.columns
        for col in colList:
            if col in columns:
                result_attr.append(col)
            else:
                for prev in tableprefixList:
                    if prev + col in columns:
                        result_attr.append(prev + col)
                        break
        print(df_result[result_attr].dropna())
        print("--- %s seconds ---" % (time.time() - start_time))
    #break

# select Date from nasdaq.csv where Open <= 5000 or High < 5200
# select Date from nasdaq.csv, euro50.csv
# select * from nasdaq.csv, euro50.csv
# select Date from nasdaq.csv where Open = 4000
# TODO select * from test1.csv where NOT String = 'hui'
# select * from test1.csv where String like '_hui%'
# SELECT * FROM movies.csv WHERE title_year = 1999
# SELECT title_year FROM movies.csv WHERE NOT title_year = 1999
# SELECT movie_title,imdb_score FROM movies.csv WHERE movie_title LIKE '%Harry_Potter%'
# SELECT title_year,movie_title,award,imdb_score FROM movies.csv M, oscars.csv A WHERE M.movie_title = A.Film AND M.imdb_score < 7
# SELECT title_year,movie_title,award,imdb_score FROM movies.csv M, oscars.csv A WHERE ( M.movie_title = A.Film AND M.imdb_score < 7 )
# SELECT title_year,movie_title,award,imdb_score FROM movies.csv M, oscars.csv A WHERE  M.movie_title = A.Film AND NOT M.imdb_score < 7
# SELECT title_year,movie_title,award,imdb_score FROM movies.csv M, oscars.csv A WHERE  ( M.movie_title = A.Film ) AND NOT ( M.imdb_score < 7 AND  M.title_year > 2000 )
# SELECT title_year,movie_title,award,imdb_score FROM movies.csv M, oscars.csv A WHERE  ( M.movie_title = A.Film ) AND NOT ( M.imdb_score < 7 OR  M.title_year > 2000 )
# SELECT M1.director_name,M1.title_year,M1.movie_title,M2.title_year,M2.movie_title,M3.title_year,M3.movie_title FROM movies.csv M1, movies.csv M2, movies.csv M3 WHERE M1.director_name = M2.director_name AND M1.director_name = M3.director_name AND M1.movie_title <> M2.movie_title AND M2.movie_title <> M3.movie_title AND M1.movie_title <> M3.movie_title AND M1.title_year < M2.title_year-10 AND M2.title_year < M3.title_year-10
