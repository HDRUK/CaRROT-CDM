import scipy.stats
import time
import numpy as np
import json

def create_analysis(_filter):
    def ana(model):
        df = model.filter(_filter,dropna=True)
        if len(df) == 0:
            return {'fit':None,'hist':None}

        df['age'] = 2022 - df['year_of_birth']
        res = scipy.stats.linregress(x=df['age'],y=df['value_as_number'])
        attributes = ['intercept', 'intercept_stderr', 'pvalue', 'rvalue', 'slope', 'stderr']
        res = {a:getattr(res,a) for a in attributes}
        res.update({'n':len(df)})
        antibodies = df['value_as_number']
        hist = np.histogram(antibodies,bins=20, range=(0,300))
        
        #time.sleep(10)
        return {'fit':res,'hist':hist}

        # #time.sleep(20)
        # fname = 'res_'+hex(id(_filter))+'.json'
        # with open(fname,"w") as f:
        #     json.dump(res,f,indent=6)
        # return res

    return ana
