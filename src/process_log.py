import sys
arguments = sys.argv

input_paths = [item for item in arguments if 'input' in item]
output_paths = [item for item in arguments if 'output' in item]
print(input_paths)
print(output_paths)

import os
import re
import sys
import io
from collections import defaultdict
from dateutil import parser
import datetime
import pandas as pd
#with io.open(file_name, 'r', encoding='windows-1252') as twitter_file:

Feature1_dict=defaultdict(int)
Feature1_output_key=[]
Feature2_dict=defaultdict(int)
Feature2_output_key=[]
Feature3_dict={}
timestamp_records=[]
Feature4_dict=defaultdict(list)
Feature4_output=[]
list_time_temp=[]

err_lines=[]

i=0
with io.open(input_paths[0], 'rb') as f:
    for line in f:
            try:
                #print(line)
                host = line.split()[0]
                #print(host)
                Feature1_dict[host] +=1
                
                timestamp = re.findall(r'\[(.+?)\]', line)[0]
                #print(timestamp)
                colon_location= timestamp.find(':')
                timestamp_edit = timestamp[:11]+' '+timestamp[11+1:]
                timeofday=parser.parse(timestamp_edit)
                timestamp_records.append(timeofday)
                
                
                
                request = re.findall(r'''"(.*?)"+\s+''', line)
                
                QuotationMark_location = [x.start() for x in re.finditer('"', line)]
                for item in QuotationMark_location[1:-1]:
                    line=line[:item]+line[item+1:]
                
                request = re.findall(r'''"(.*?)"+\s+''', line)[0]
                #print(request)
 
                httpCode = line.split()[-2]
                #print(httpCode)
        
                bytes = line.split()[-1]
                #print(bytes)
                
                if 'GET' in request:
                    request_resource = request.split(' ')[1]
                    #print(request_split)
                    try:
                        Feature2_dict[request_resource] += int(bytes)
                    except:
                        pass


                
                #Feature_4
                if httpCode=='401': #failed login attempt http code 401
                    Feature4_dict[host].append(timeofday)
                    time_test = Feature4_dict[host]
                    if len(time_test)>2:
                        if time_test[-1]-time_test[0]<datetime.timedelta(0,21):
                            Feature4_output.append(line)
                            
                    if time_test[-1]-time_test[0]>datetime.timedelta(0,20):
                        Feature4_dict[host]=(Feature4_dict[host])[-1]
                except:
                    err_lines.append(line)
                    pass
     
                i+=1
    
#Feature_3
df=pd.DataFrame({'timeSTAMPS':timestamp_records})
df.index = pd.to_datetime(df.timeSTAMPS)
df.columns=['count']
df['count']=1
df_resampled=pd.DataFrame(df['count'].resample('60S').sum())
df_rolling = (df_resampled.rolling('60T').sum())
df_sort=df_rolling.sort_values('count', ascending=False)
df_sort = df_sort[:10]

df_rolling=df_resampled=df=None
    
    
#output
#------
#Feature1
Feature1_output_key=sorted(Feature1_dict, key=lambda key: (-Feature1_dict[key], key))[:10]
with open(output_paths[0], 'w') as f:
    for item in Feature1_output_key:
        text=item+', '+str(Feature1_dict[item])+'\n'
        f.write(text)
f.close()


#Feature2
Feature2_output_key=sorted(Feature2_dict, key=lambda key: (-Feature2_dict[key], key))[:10]
with open(output_paths[2], 'w') as f:
    for item in Feature2_output_key:
        text=item+'\n'
        f.write(text)
f.close()


#Feature_3
with open(output_paths[1], 'w') as f:
    for key in df_sort.index: 
        text=key.strftime('%d/%b/%Y:%H:%M:%S %z')+ ', ' +str(int((df_sort.loc[df_sort.index==key])['count'][0]))+'\n'
        f.write(text)
f.close()


#Feature_3
with open(output_paths[-1], 'w') as f:
    for item in Feature4_output:
        text=item
        f.write(text)

f.close()
