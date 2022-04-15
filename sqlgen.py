import pandas as pd
import json
import sys
import os
import glob
from datetime import datetime

dt = datetime.now().strftime("%Y%m%d-%I%M%S%p")



## Pre Process
def trim_all_columns(dfx):
    return dfx.applymap(lambda xx: xx.strip() if isinstance(xx,str) else xx)

df = pd.read_excel("/Users/mudgalfamily/META.XLS").fillna('NA')
df = df[(df['COUNTRY'] != 'NA') | (df['SERVICE_NAME'] != 'NA')]
df = trim_all_columns(df)
#print(len(df))

df1 = df.groupby(['COUNTRY','SERVICE_NAME'], as_index=False, sort=False).agg(list)
df2 = df1.to_dict(orient='records')
#print(df2)

interim_json = open("service_{date}.json".format(date=dt),'w')

## SQL Gen
rqst = []
sql = []
status = []
msg = []
srvc = []
country = []
active = []
target_table =[]

final_d = {'Country': country, 'Service': rqst, 'SQL':sql, 
            'Status':status, 'Message':msg, 
            'Active':active}

def sqlgen(**kargs):
    #print('In function....')
    #print(len(kargs['linkages']))
    #print(len(kargs['tables_with_schema']))
    if len(kargs['linkages']) > len(kargs['tables_with_schema'])-1 :
        sql.append("None")
        status.append("Failed")
        msg.append("Disc in number of links and tables provides")

    elif len(kargs['target_table']) > 1:
        sql.append("None")
        status.append("Failed")
        msg.append("Cannot have more than one target table")
    
    elif 'NA' in kargs['source_schema']:
        sql.append("None")
        status.append("Failed")
        msg.append("Table schema must be present")

    elif len(kargs['base_table_list']) > 1:
        sql.append("None")
        status.append("Failed")
        msg.append("Base tables cannot be more than one ")

    elif base_table_flag_chk and len(kargs['tables']) > 1:
        sql.append("None")
        status.append("Failed")
        msg.append("Base table is required for multi-tables build")

    elif len(kargs['filters']) > 1:
        sql.append("None")
        status.append("Failed")
        msg.append("Filters are mandatory")

    elif kargs['join_check']:
        sql.append("None")
        status.append("Failed")
        msg.append("Number of joins defined by linakges does not match")

    else:
        groupby_idx = []
        groupby_cols_pre = []
        groupby_cols_post = []
        aggr_cols = []
        group_filters = []

        # Get index of groupby fields
        for e1,y1 in enumerate(kargs['aggr']):
            groupby_idx.append(e1) if y1 != 'NA' else None
            #print(groupby_idx)
        # Column list of non-group attributes (select clause)
        for e2,y2 in enumerate(kargs['cols_alias']):
            groupby_cols_pre.append(y2) if e2 not in groupby_idx else None
            #print(groupby_cols_pre)
        # Group columns (group clause)
        for e3,y3 in enumerate(kargs['source_columns']):
            groupby_cols_post.append(y3) if e3 not in groupby_idx else None
            #print(groupby_cols_post)
        # Prepare aggr in select clause
        for r in groupby_idx:
            groupedcol = kargs['derived_logic'][r] if kargs['direct_derived'][r] == 'DERIVED' else kargs['source_columns'][r]
            #print(groupedcol)
            aggr_cols.append(kargs['aggr'][r] + '('+groupedcol+')'+' as '+kargs['aggr'][r]+'_'+kargs['alias_cols'][r])
            #print(aggr_cols)
        # Prepare grouped filters
        for s1 in kargs['grp_filters']:
            group_filters.append(s1) if s1 != 'NA' else None

        #print('In function....')
        if len(kargs['join_table']) == 0: # SQL with single table
            if 'Y' in kargs['groupby']: #SQL with group by 
                dff ='select ' + ', '.join(jj for jj in aggr_cols) + ', ' + ', '.join(kk for kk in groupby_cols_pre) + \
                    ' from ' + 'join '.join(i1 for i1 in kargs['tables_with_schema'])+' where ' + \
                        ' and '.join(i2 for i2 in kargs['filters'])+' group by '+ \
                            ', '.join(ll for ll in groupby_cols_post) \
                                + ((' having  '+ ' and '.join(t2 for t2 in group_filters)) if any(z!='NA' for z in kargs['grp_filters']) else '')
                sql.append(dff)
            else: #SQL without group by
                dff = 'select '+ ', '.join(i3 for i3 in kargs['cols_alias']) + \
                    ' from ' + ' join '.join(i4 for i4 in kargs['tables_with_schema']) + \
                        ' where ' + ' and '.join(i5 for i5 in kargs['filters'])
                sql.append(dff)
        else: # SQL with multiple tables
            if 'Y' in kargs['groupby']: #SQL with group by 
                dff ='select ' + ', '.join(jj for jj in aggr_cols) + ', ' + ', '.join(kk for kk in groupby_cols_pre) + \
                    ' from ' + kargs['join_statement'] +' where ' + \
                        ' and '.join(i2 for i2 in kargs['filters'])+' group by '+ \
                            ', '.join(ll for ll in groupby_cols_post) \
                                + ((' having  '+ ' and '.join(t2 for t2 in group_filters)) if any(z!='NA' for z in kargs['grp_filters']) else '')             
                sql.append(dff)
            else: #SQL without group by
                dff = 'select '+ ', '.join(i3 for i3 in kargs['cols_alias']) + \
                    ' from ' + kargs['join_statement'] + \
                        ' where ' + ' and '.join(i5 for i5 in kargs['filters'])
                sql.append(dff)
        
        status.append("Success")
        msg.append("SQL Created Successfully")



for d in df2:
    #print(d['SOURCE_ATTR'])

    d['ACTIVE'] = list(set(d['ACTIVE']))
    d['ACTIVE'].remove('NA') if 'NA' in d['ACTIVE'] else d['ACTIVE']

    d['TARGET_TABLE'] = list(set(d['TARGET_TABLE']))
    d['TARGET_TABLE'].remove('NA') if 'NA' in d['TARGET_TABLE'] else d['TARGET_TABLE']


    if set(d['UNION']) == {'NA'}:
        ##print('In not Union...')

        try:

            tables_with_schema = []
            for a,b in enumerate(d['SOURCE_SCHEMA']):
                tables_with_schema.append(d['SOURCE_SCHEMA'][a].strip()+'.'+d['SOURCE_TABLE'][a].strip())
            #print(tables_with_schema)

            d['LINKAGES'] = list(dict.fromkeys(d['LINKAGES']))
            d['LINKAGES'].remove('NA')
            #print('LINKAGE:{}'.format(d['LINKAGES']))
            
            d['JOIN_TABLE'] = list(dict.fromkeys(d['JOIN_TABLE']))
            d['JOIN_TABLE'].remove('NA')
            #print('JOIN_TABLE:{}'.format(d['JOIN_TABLE']))

            base_table = None
            base_table_list = []
            other_tables = []
            join_type = []
            join_statement = []

            for m,n in enumerate(d['BASE_TABLE_FL']):
                if n.strip() == 'Y':
                    base_table = tables_with_schema[m]
                    base_table_list.append(tables_with_schema[m])
                    #print('BASE:{}' .format(base_table_list))

            

            for s in d['JOIN_TABLE']:
                other_tables.append(s)
                #print(other_tables)

            for t in d['JOIN_TYPE']:
                if t.lower() == 'left':
                    join_type.append('left outer join')
                elif t.lower() == 'right':
                    join_type.append('right outer join')
                elif t.lower() == 'inner':
                    join_type.append('join')

            tables_with_join_statement = []

            join_check = None

            if (len(d['SOURCE_TABLE']) > 1) and len(join_type) == len(d['JOIN_TABLE']) == len(d['LINKAGES']):
                join_check = False
                for i, j in enumerate(other_tables):
                    tables_with_join_statement.append(' '+join_type[i]+' '+other_tables[i]+' ON '+d['LINKAGES'][i])
                    #print(tables_with_join_statement)
            else:
                join_check = True

            base_table_flag_chk = None

            if 'Y' in d['BASE_TABLE_FL']:
                join_statement = base_table + ''.join(tables_with_join_statement)
                #print(join_statement)
                base_table_flag_chk = False
            else:
                base_table_flag_chk = True

            d['FILTERS'] = list(set(d['FILTERS']))
            d['FILTERS'].remove('NA')
            #print(d['FILTERS'])

            column_with_alias = []
            #print('checkpoint')
            #print(d['SOURCE_ATTR'])
            for i in range(len(d['SOURCE_ATTR'])):
                #print(i)
                columns_with_derived_logic = d['DERIVATION_RULE'][i] if d['DIRECT_DERIVED'][i] == 'DERIVED' else d['SOURCE_ATTR'][i]
                #print(columns_with_derived_logic)
                new_cols = columns_with_derived_logic + ' as ' + d['TARGET_ATTR'][i]
                #print(new_cols)
                column_with_alias.append(new_cols)
                #print(column_with_alias)

            #print('starting sql')

            sqlgen(requests=d['SERVICE_NAME'],
                country=d['COUNTRY'],
                source_schema=d['SOURCE_SCHEMA'],
                tables=d['SOURCE_TABLE'], 
                source_columns=d['SOURCE_ATTR'],
                cols_alias = column_with_alias, 
                linkages = d['LINKAGES'], 
                filters = d['FILTERS'],
                groupby=d['GROUPBY'],
                aggr=d['AGGR'],
                grp_filters=d['GROUP_FILTERS'],
                tables_with_schema=tables_with_schema,
                join_type=d['JOIN_TYPE'],
                join_table=d['JOIN_TABLE'],
                join_statement=join_statement,
                target_table=d['TARGET_TABLE'],
                base_table_flag_chk=base_table_flag_chk,
                base_table_list=base_table_list,
                join_check=join_check,
                derived_logic=d['DERIVATION_RULE'],
                direct_derived=d['DIRECT_DERIVED'],
                alias_cols=d['TARGET_ATTR']
                )

            rqst.append(d['SERVICE_NAME'])
            country.append(d['COUNTRY'])
            active.append(d['ACTIVE'])
            target_table.append(d['TARGET_TABLE'])

            #parsed = json.loads(d)
            j = json.dumps(d, indent=4, sort_keys=True)

            interim_json.write(j + '\n')
            #print(sql)
            #print(msg)
            #print(final_d)

        except Exception as x:
            template = "An exception of type {0} occured. Arguments :{1 !r}"
            message =template.format(type(x).__name__,x.args)
            sql.append("None")
            status.append("Failed")
            msg.append(message)
            rqst.append(d['SERVICE_NAME'])
            country.append(d['COUNTRY'])
            target_table.append(d['TARGET_TABLE'][0])
            active.append(d['ACTIVE'][0])

            interim_json.write(str(d) + '\n' 'Message: {}'.format(final_d['Message']) + '\n')

    else:
        #print('third query')
        indcs = []
        union = []

        # Check if services exisit 

        check = all(item.strip() in final_d['Service'] for item in d['UNION'][0].split(','))

        # Pick service names 
        for i in d['UNION'][0].split(','):
            union.append(i)

        # get indices of union services
        for i,x in enumerate(final_d['Service']):
            if x in union:
                indcs.append(i)
        #print(indcs)

        if check:

            if all(final_d['SQL'][i] != 'None' for i in indcs):

                sql.append(' union '.join(final_d['SQL'][i] for i in indcs))
                #print('union working')
                status.append("Success")
                msg.append("SQL created successfully (with Union)")

            else:

                sql.append("None")
                status.append("Failed")
                msg.append("SQLs not available for one/many required services {} (for union)".format(d['UNION']))
        else:

            sql.append("None")
            status.append("Failed")
            msg.append("SQLs not available for one/many required services {} (for union)".format(d['UNION']))


        rqst.append(d['SERVICE_NAME'])
        country.append(d['COUNTRY'])
        active.append(d['ACTIVE'])
        target_table.append(d['TARGET_TABLE'])

        interim_json.write(str(d) + '\n' 'Message: {}'.format(final_d['Message']) + '\n')

interim_json.close()

for i,j in enumerate(final_d['Service']):
    a = {'Country':final_d['Country'][i],
    'Service':final_d['Service'][i],
    'SQL':final_d['SQL'][i],
    'Status':final_d['Status'][i],
    'Message':final_d['Message'][i]}

    service_SQL ="{prefix}_sql_{cname}_{service}_{dt}.json".format(prefix='OPS',\
                                                            cname=final_d['Country'][i],\
                                                            service=final_d['Service'][i],dt=dt)
    k = open(service_SQL,'w')
    json.dump(a, k, indent=4)


