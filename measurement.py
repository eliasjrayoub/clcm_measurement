def measurement(fd):
    import math
    import pandas as pd

    fd = fd.copy()
    
    df = fd.groupby(['control']).sum()
    df = df.drop(['act_discount', 'act_items'], axis=1)
    df = df.reset_index()
    
  

    inc_sales = (df.loc[0, 'act_sales'])-(df.loc[1, 'act_sales']/df.loc[1,'contacted'])*df.loc[0,'contacted']
    uplift = inc_sales/(df.loc[0, 'act_sales']-inc_sales)

    df['ATV'] = (df['act_sales'] / df['act_trans']).round(2)
    df['ACV'] = (df['act_sales'] / df['active']).round(2)
    df['ATF'] = (df['act_trans'] / df['active']).round(2)
    df['active_rate'] = (df['active'] / df['contacted'])
    #control_table['active_rate'] = "%.2d" % [control_table['active'] / control_table['contacted']] 
    df['Incremental Sales'] = [inc_sales, ""]
    df['Uplift'] = [uplift, ""]

    #BREAKING DOWN UPLIFT
    #atf_brk = inc_sales * (math.log10(df.loc[0,'ATF']/df.loc[1,'ATF'])/math.log10((df.loc[0,'act_sales']/df.loc[0,'contacted'])/(df.loc[1,'act_sales']/df.loc[1,'contacted'])))
    #atf_pct = atf_brk/inc_sales*uplift

    #atv_brk = (inc_sales*(math.log10(df.loc[0,'ATV']/df.loc[1,'ATV'])/math.log10((df.loc[0,'act_sales']/df.loc[0,'contacted'])/(df.loc[1,'act_sales']/df.loc[1,'contacted'])))).astype(float)
    #atv_pct = (atv_brk/inc_sales*uplift).astype(float)

    #activity_brk = inc_sales*(math.log10(df.loc[0,'active_rate']/df.loc[1,'active_rate'])/math.log10((df.loc[0,'act_sales']/df.loc[0,'contacted'])/(df.loc[1,'act_sales']/df.loc[1,'contacted'])))
    #activity_pct = activity_brk/inc_sales*uplift

    # df['ATF Uplift'] = ["%.2d" % atf_brk, ""]
    # df['ATF Uplift %']= ["%.2d" % atf_pct, ""]
    # df['ATV Uplift'] = ["%.2d" % atv_brk, ""]
    # df['ATV Uplift %'] = [(atv_pct.round(2)), ""]
    # df['Activity Rate Uplift'] = ["%.2d" % activity_brk, ""]
    # df['Activity Rate uplift %'] = ["%.2d" % activity_pct, ""]

    df = df.reset_index()

    df.control = pd.Series(['Test', 'Control'])
    df = df.rename(columns={'contacted':'Universe', 'active':'Active', 'act_sales':'Sales', 'act_trans':'Transactions', 'active_rate':'Active Rate'})


    #uplift = inc_sales/(df['act_sales'].loc['N']-inc_sales)
    #inc_sales = df['act_sales'].loc['N'] - (df['contacted'].loc['N'] * df['act_sales'].loc['Y']/ df['contacted'].loc['Y'])

    df.set_index('control')
    
    return df 



def measurement2(fd):
    # ACV, ATF are calculated with active customers in the denominator
    import math     
    import pandas as pd

    df = fd.copy()
    
    df = df.groupby(['control']).sum()
    df = df.reset_index()
    
    df.loc[df.control == 'Y', 'control'] = 'Control'
    df.loc[df.control == 'N', 'control'] = 'Test'
    df.index = df.control
    del df['control']
    

    inc_sales = ((df.loc['Test', 'act_sales']/df.loc['Test', 'contacted'])-(df.loc['Control', 'act_sales']/df.loc['Control', 'contacted']))*(df.loc['Test', 'contacted'])
    uplift = inc_sales/(df.loc['Test', 'act_sales']-inc_sales)

    df['ATV'] = (df['act_sales'] / df['act_trans']).round(0)
    df['ACV'] = (df['act_sales'] / df['active']).round(0)
    df['ATF'] = (df['act_trans'] / df['active']).round(2)
    df['active_rate'] = (df['active'] / df['contacted'])
    #control_table['active_rate'] = "%.2d" % [control_table['active'] / control_table['contacted']] 
    df['Incremental Sales'] = [inc_sales, ""]
    df['Uplift'] = [uplift, ""]

  

    df = df.rename(columns={'contacted':'Universe', 'active':'Active', 'act_sales':'Sales',
                            'act_trans':'Transactions', 'active_rate':'Active Rate'})
  

    new_df = pd.DataFrame(
    {
    "Metric" : ['Universe', 'Active Members','Active Rate', 'Total Sales', 'Total Transactions', 
                'Total Discount',  'ATV', 'ATF', 'ACV', 'Incremental Sales', 'Uplift'],

    "Units" : ['Members', 'Members','%','Euro', 'Number', 'Euro',  
                'Euro/Trans', 'Trans/Member', 'Euro/Member', 'Euro', '%' ],
    
    "Target" : ["%.2d" % round(df.loc['Test','Universe'],0), #the %.2d format removes unwanted 0s from the decimal
                "%.2d" % round(df.loc['Test','Active'],0),
                "%.1f" % round((df.loc['Test','Active Rate']*100),0) + "%",
                "%.2d" % round(df.loc['Test','Sales'],0),
                "%.2d" % round(df.loc['Test','Transactions'],0), 
                "%.2d" % round(df.loc['Test','act_discount'],0), 
                "%.2d" % round(df.loc['Test','ATV'],0), 
                "%.2f" % round(df.loc['Test','ATF'],2), 
                "%.2d" % round(df.loc['Test','ACV'],0),
                "%.2d" % round(df.loc['Test','Incremental Sales'],0), 
                "%.2f" % round((df.loc['Test','Uplift'])*100,2) + "%"
               ],
    
    "Control" : ["%.2d" % round(df.loc['Control','Universe'],0), 
                 "%.2d" % round(df.loc['Control','Active'],0), 
                 "%.1f" % round(df.loc['Control','Active Rate']*100,0) + "%", 
                 "%.2d" % round(df.loc['Control','Sales'],0), 
                 "%.2d" % round(df.loc['Control','Transactions'],0), 
                 "%.2d" % round(df.loc['Control','act_discount'],0), 
                 "%.2d" % round(df.loc['Control','ATV'],0), 
                 "%.2f" % round(df.loc['Control','ATF'],2), 
                 "%.2d" % round(df.loc['Control','ACV'],0),
                 df.loc['Control','Incremental Sales'], 
                 df.loc['Control','Uplift']
                ]
    
    })

    new_df = new_df[['Metric', 'Units', 'Target', 'Control']]
    new_df.index=new_df.Metric
    del new_df['Metric']
    return new_df


 #norm


def measurement3(fd):


    # ACV, ATF are calculated with active customers in the denominator
    import math     
    import pandas as pd

    df = fd.copy()
    
    df = fd.groupby(['control']).sum()
    df = df.reset_index()
    
    df.loc[df.control == 'Y', 'control'] = 'Control'
    df.loc[df.control == 'N', 'control'] = 'Test'
    df.index = df.control
    del df['control']
    
    inc_sales = (df.loc['Test', 'act_sales'])-(df.loc['Control', 'act_sales']/df.loc['Control','contacted'])*df.loc['Test','contacted']

    #inc_sales = (df.loc['Test', 'act'])
    uplift = inc_sales/(df.loc['Test', 'act_sales']-inc_sales)

    df['ATV'] = (df['act_sales'] / df['act_trans']).round(0)
    df['ACV'] = (df['act_sales'] / df['active']).round(0)
    df['ATF'] = (df['act_trans'] / df['active']).round(2)
    df['active_rate'] = (df['active'] / df['contacted'])
    #control_table['active_rate'] = "%.2d" % [control_table['active'] / control_table['contacted']] 
    df['Incremental Sales'] = [inc_sales, ""]
    df['Uplift'] = [uplift, ""]

  

    df = df.rename(columns={'contacted':'Universe', 'active':'Active', 'act_sales':'Sales',
                            'act_trans':'Transactions', 'active_rate':'Active Rate'})
  

    new_df = pd.DataFrame(
    {
    "Metric" : ['Universe', 'Active Members','Active Rate', 'Total Sales', 'Total Transactions', 
                'Total Discount',  'ATV', 'ATF', 'ACV', 'Incremental Sales', 'Uplift'],

    "Units" : ['Members', 'Members','%','Euro', 'Number', 'Euro',  
                'Euro/Trans', 'Trans/Member', 'Euro/Member', 'Euro', '%' ],
    
    "Target" : ["%.2d" % round(df.loc['Test','Universe'],0), #the %.2d format removes unwanted 0s from the decimal
                "%.2d" % round(df.loc['Test','Active'],0),
                "%.2d" % round((df.loc['Test','Active Rate'])*100.00,0) + "%",
                "%.2d" % round(df.loc['Test','Sales'],0),
                "%.2d" % round(df.loc['Test','Transactions'],0), 
                "%.2d" % round(df.loc['Test','act_discount'],0), 
                "%.2d" % round(df.loc['Test','ATV'],0), 
                "%.2f" % round(df.loc['Test','ATF'],2), 
                "%.2d" % round(df.loc['Test','ACV'],0),
                "%.2d" % round(df.loc['Test','Incremental Sales'],0), 
                "%.2f" % round((df.loc['Test','Uplift'])*100,2) + "%"
               ],
    
    "Control" : ["%.2d" % round(df.loc['Control','Universe'],0), 
                 "%.2d" % round(df.loc['Control','Active'],0), 
                 "%.2d" % round((df.loc['Control','Active Rate'])*100,0) + "%", 
                 "%.2d" % round(df.loc['Control','Sales'],0), 
                 "%.2d" % round(df.loc['Control','Transactions'],0), 
                 "%.2d" % round(df.loc['Control','act_discount'],0), 
                 "%.2d" % round(df.loc['Control','ATV'],0), 
                 "%.2f" % round(df.loc['Control','ATF'],2), 
                 "%.2d" % round(df.loc['Control','ACV'],0),
                 df.loc['Control','Incremental Sales'], 
                 df.loc['Control','Uplift']
                ]
    
    })

    new_df = new_df[['Metric', 'Units', 'Target', 'Control']]
    new_df.index=new_df.Metric
    del new_df['Metric']
    return new_df



def normalize(raw, start_dt, period_days):
    #raw is the table outputed from the first query run to get measurement data 
    #start_dt is the start date of the campaign - this function 

    import queries
    import measurement as mt 
    from datetime import date
    from dateutil.relativedelta import relativedelta
    from datetime import datetime
    date_format = "%Y%m%d"

    a = datetime.strptime(str(start_dt), date_format)

    norm_start = a - relativedelta(days=period_days) #range of pre-period 
    norm_end = a - relativedelta(days=1)
    norm_start = norm_start.strftime("%Y%m%d")
    norm_end = norm_end.strftime("%Y%m%d") ##good so far!
    

    campaign_name = raw.loc[0,'campaign_name']
    segments = raw.loc[:,'segment_name'].unique().tolist()
    treatments = raw.loc[:,'treatment_name'].unique().tolist() 
    
    id_seg = " or ".join(["segment_name like '{seg_name}'".format(seg_name=seg_name) for seg_name in segments])
    id_treatment = " or ".join(["treatment_name like '{id_name}'".format(id_name=id_name) for id_name in treatments])

    pre_period = Query(queries.norm_data(campaign_name, norm_start, norm_end, id_treatment, id_seg))
    pre_period = remove_dup(pre_period)
    
    current_metrics = mt.measurement2(raw)
    pre_metrics = mt.measurement2(pre_period)
    
    
    sales_t = float(current_metrics.loc['Total Sales', 'Target'])
    acv_c = float(current_metrics.loc['ACV', 'Control'])
    universe_t = float(current_metrics.loc['Universe', 'Target'])
    
    x = float(pre_metrics.loc['Total Sales', 'Target'])/float(pre_metrics.loc['Universe','Target'])
    y = float(pre_metrics.loc['Total Sales', 'Control'])/float(pre_metrics.loc['Universe', 'Control'])
    factor=x/y
    inc_sales_normalized = sales_t - (acv_c*factor) * universe_t
    uplift_normalized = inc_sales_normalized/((float(current_metrics.loc['Total Sales', 'Target']))-inc_sales_normalized)
    
    current_metrics.loc['Incremental Sales', 'Target'] = inc_sales_normalized
    current_metrics.loc['Uplift', 'Target'] = uplift_normalized
              
    return current_metrics


# def rfm_incremental(rfm_output_table):
