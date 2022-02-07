import scipy.stats
import time
import json

def create_analysis(_filter):
    def ana(model):
        df = model.filter(_filter,dropna=True)
        df['age'] = 2022 - df['year_of_birth']
        res = scipy.stats.linregress(x=df['age'],y=df['value_as_number'])
        attributes = ['intercept', 'intercept_stderr', 'pvalue', 'rvalue', 'slope', 'stderr']
        res = {a:getattr(res,a) for a in attributes}
        res.update({'n':len(df)})
        #time.sleep(20)
        fname = 'res_'+hex(id(_filter))+'.json'
        with open(fname,"w") as f:
            json.dump(res,f,indent=6)
        return res
    return ana
