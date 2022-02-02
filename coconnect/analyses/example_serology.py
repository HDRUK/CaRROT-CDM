import scipy.stats

def create_analysis(_filter):
    def ana(model):
        df = model.filter(_filter,dropna=True)
        df['age'] = 2022 - df['year_of_birth']
        res = scipy.stats.linregress(x=df['age'],y=df['value_as_number'])
        attributes = ['intercept', 'intercept_stderr', 'pvalue', 'rvalue', 'slope', 'stderr']
        res = {a:getattr(res,a) for a in attributes}
        res.update({'n':len(df)})
        return res
    return ana
