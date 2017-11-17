import pandas as pd
import numpy as np
import re


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

while True:
    df_result = pd.DataFrame({'A' : []})
    command = input("Type the your SQL and press Enter.")
    #command = 'SELECT title_year,movie_title,Award,imdb_score FROM movies.csv M, oscars.csv A WHERE M.movie_title = A.Film AND M.imdb_score < 7'
    if len(command) < 1:  # check prevents a crash when indexing to 1st character
       continue
    sqlList = command.split() #Create the list to store the SQL
    colList = sqlList[1].split(',')
    attributeList = list(colList)
    # print(colList)

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
    tableposList.append(conPos-4) #append the position of WHERE as the last value in list

    #find the tables needed to be renamed.
    renametableList = [] #Save the tables needed to be renamed
    tableprefixList = [] #Save the prefix of each table
    for i in range(len(tableposList)):
        if i > 0 and tableposList[i]-tableposList[i-1] == 2:
            renametableList.append(tableList[tableposList[i-1]])

    #Save the prefix of each table(renamed or not)
    j = 0
    for i in range(len(tableposList)-1):

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
    isAttribute = []

    if len(fileList) == 1:
        df_result = pd.read_csv(fileList[0])#, keep_default_na=False)
        isAttribute = set(df_result.columns)

    else:
        for i in range(len(fileList)):

            df1 = pd.read_csv(fileList[i])#, keep_default_na=False)

            if colList != ['*']:
                required_columns = set()
                columns = df1.columns
                # print(columns)
                for attr in attributeList:
                    if attr in columns:
                        required_columns.add(attr)
                        isAttribute.append(attr)
                    if '.' in attr and attr.split('.')[1] in columns:
                        required_columns.add(attr.split('.')[1])
                        isAttribute.append(attr)
                # print(required_columns)
                df1 = df1[list(required_columns)]
            if len(renametableList) > 0:
                columns = list(df1.columns)
                for j in range(len(columns)):
                    columns[j] = tableprefixList[i] + columns[j]
                df1.columns = columns
            df1['key'] = 0
            if df_result.empty:
                df_result = df1
            else:
                df_result = df_result.merge(df1, how = 'outer', on = 'key')
                #df_result.drop('key', 1, inplace=True)

    print('isAttribute', isAttribute)
    print(list(df_result.columns))

    #WHERE: Deal with the conditions
    if conPos != -1:
        condition = sqlList[conPos:]
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
        expression = ''
        count = 0
        for cc in range(len(condition)):
            c = condition[cc]
            if c == '|' or c == '&':
                expression += (')' + c)
                count = 0
            elif c in isAttribute:
                if count == 0:
                    expression += '(df_result[\'' + c + '\']'
                else:
                    expression += 'df_result[\'' + c + '\']'
                count += 1
            else:
                if count == 1 or is_number(c) or (cc > 0 and condition[cc-1] == '.str.match('):
                    expression += c
                else:
                    expression += '\'' + c + '\''
                count += 1
        expression += ')'
        print(expression)
        #print(eval(expression))
        df_result = df_result[eval(expression)]

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
        print(df_result[result_attr])
    #break

    # select Date from nasdaq.csv where Open <= 5000 or High < 5200
    # select Date from nasdaq.csv, euro50.csv
    # select * from nasdaq.csv, euro50.csv
    # select Date from nasdaq.csv where Open = 4000
    # TODO select * from test1.csv where NOT String = 'hui'
    # select * from test1.csv where String like '_hui%'
    # SELECT * FROM movies.csv WHERE title_year = 1999
    # SELECT movie_title,imdb_score FROM movies.csv WHERE movie_title LIKE '%Harry_Potter%'
    # SELECT title_year,movie_title,award,imdb_score FROM movies.csv M, oscars.csv A WHERE M.movie_title = A.Film AND M.imdb_score < 7
    # SELECT M1.director_name,M1.title_year,M1.movie_title,M2.title_year,M2.movie_title,M3.title_year,M3.movie_title FROM movies.csv M1, movies.csv M2, movies.csv M3 WHERE M1.director_name = M2.director_name AND M1.director_name = M3.director_name AND M1.movie_title <> M2.movie_title AND M2.movie_title <> M3.movie_title AND M1.movie_title <> M3.movie_title AND M1.title_year < M2.title_year-10 AND M2.title_year < M3.title_year-10
