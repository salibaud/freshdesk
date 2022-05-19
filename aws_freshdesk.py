import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import awswrangler as wr
import requests
import json
import pandas as pd
from tqdm import tqdm

import re

def remove_emoji(string):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002500-\U00002BEF"  # chinese char
                               u"\U00002702-\U000027B0"
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\U00010000-\U0010ffff"
                               u"\u2640-\u2642"
                               u"\u2600-\u2B55"
                               u"\u200d"
                               u"\u23cf"
                               u"\u23e9"
                               u"\u231a"
                               u"\ufe0f"  # dingbats
                               u"\u3030"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

resultados = pd.DataFrame([])

x = datetime(2022, 5, 1)

date_today = datetime.now()
days = pd.date_range(x,date_today, freq='D')

np.random.seed(seed=1111)
data = np.random.randint(1, high=100, size=len(days))
df = pd.DataFrame({'test': days, 'col2': data})
df = df.set_index('test')
df = df.reset_index()

for k in tqdm(range(len(df))):
    fecha=df["test"].iloc[k].date()

    for j in range(1,10):
        try:        
            url = "https://kaufmannayuda.freshdesk.com/api/v2/search/tickets?query=\"created_at:%27"+str(fecha)+"%27\"&page="+str(j)
            payload={}
            headers = {'Authorization': 'Bearer YVJPS3BuWHFEYTFZemtKams6WA==','Cookie': '_x_w=38_3'}
            response = requests.request("GET", url, headers=headers, data=payload)


            a=json.loads(response.text)
            b=pd.DataFrame(a)


            for i in range(len(b)):
                c = pd.json_normalize(b["results"].iloc[i])
                c=c.assign(description=c['description'].str.split('<td')).explode('description')
                c["kaufmann"] = c['description'].str.find('<div style="float:right">')
                c['description'] = c['description'].str.replace('<div style="float:right">','')
                c['description'] = c['description'].str.replace('<div></div>','')
                c["busqueda"] = c['description'].str.find("</div>")

                c['user'] = c.apply(lambda x: x['description'][0:x['busqueda']],axis=1)
                c["busqueda"] = c['description'].str.find("<div")
                c['user'] = c.apply(lambda x: x['user'][x['busqueda']:250],axis=1)
                c["busqueda"] = c['user'].str.find('>')
                c['user'] = c.apply(lambda x: x['user'][x['busqueda']+1:250],axis=1)
                c['user_kauf'] = np.where(c["kaufmann"]!=-1,c['user'],'')
                c['user'] = np.where(c["kaufmann"]==-1,c['user'],'')

                cc=(c[2:])[:-2]

                c = pd.json_normalize(b["results"].iloc[i])
                file = open("sample.html", "w+", errors="ignore")
                file.write(str(c["description"].iloc[0]))
                file.close()
                d=pd.read_html("sample.html")
                e=d[1].rename(columns={0:"conversacion"})

                f=pd.concat([e.reset_index(),cc[["user","user_kauf","custom_fields.cf_telfono_mvil","tags"]].reset_index(drop='True')],axis=1)

                descrip = pd.json_normalize(b["results"].iloc[i])

                f["id"] = descrip["id"].iloc[0]
                f["due_by"] = descrip["due_by"].iloc[0]
                f["requester_id"] = descrip["requester_id"].iloc[0]
                f["responder_id"] = descrip["responder_id"].iloc[0]
                f["custom_fields_cf_tipo_consulta_inicial"] = descrip["custom_fields.cf_tipo_consulta_inicial"].iloc[0]
                f["custom_fields_cf_categ_caso_p1"] = descrip["custom_fields.cf_categ_caso_p1"].iloc[0]
                f["custom_fields_cf_categ_caso_p2"] = descrip["custom_fields.cf_categ_caso_p2"].iloc[0]
                f["custom_fields_cf_lnea_de_negocio"] = descrip["custom_fields.cf_lnea_de_negocio"].iloc[0]
                f["custom_fields_cf_tipo_de_consulta"] = descrip["custom_fields.cf_tipo_de_consulta"].iloc[0]
                f["custom_fields_cf_area_de_negocio"] = descrip["custom_fields.cf_area_de_negocio"].iloc[0]
                f["custom_fields_cf_consulta_quick_reply"] = descrip["custom_fields.cf_consulta_quick_reply"].iloc[0]
                f=f.rename(columns={"custom_fields.cf_telfono_mvil":"custom_fields_cf_telfono_mvil"})
                #f["tags"] = descrip["tags"].iloc[0]

                resultados = resultados.append(f)

        except:
            pass


resultados["user"]=resultados["user"].str.replace('\d+', '',regex=True)
resultados["user"]=resultados["user"].str.replace('&', '',regex=True)
resultados["user"]=resultados["user"].str.replace('#', '',regex=True)
resultados["user"]=resultados["user"].str.replace(';', '',regex=True)


wr.s3.to_parquet(df=(resultados),
             path="s3://kaufmann-data-science/freshdesk",
             dataset=True,
             mode = 'append')
