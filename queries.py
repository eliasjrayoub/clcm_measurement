# coding=ascii

def pca_data(campaign_name, start_dt, end_dt, id_treatment='%', id_seg ='%'):
    
    query = """
    with contacts as( 
    select distinct campaign_name, treatment_name, segment_name, 
    
    case when control_group_flag='Y' 
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    or segment_name like '%Control%'
    or campaign_name like '%CT%'
    
    then 'Y' else 'N' end as control,

    contact_key, campaign_start_dt, campaign_end_dt

    from rawdata.b_camp_offer_rk a join rawdata.b_camp_contact_rk b using (camp_offer_key)

    where campaign_name like '{campaign_name}'
    and {id_seg}
    and {id_treatment}
    ),

    segment as(
    select distinct campaign_name, treatment_name, segment_name, 
    case when control_group_flag='Y' 
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    or segment_name like '%Control%'
    or campaign_name like '%CT%'
      
    then 'Y' else 'N' end as control,
    count(distinct contact_key) as contacted 

    from rawdata.b_camp_offer_rk join rawdata.b_camp_contact_rk using (camp_offer_key)

    where campaign_name like '{campaign_name}'
    and {id_seg}
    and {id_treatment}

    group by 1,2,3,4
    ),
 
    trans as (
    select * from rawdata.b_transaction_rk
    where transaction_dt_key >= {start_dt} and transaction_dt_key <= {end_dt}
    and contact_key > 0
    and transaction_type_name = 'Item'
    )

    select campaign_name, treatment_name, segment_name, control, contacted,
    count(distinct b.contact_key)::int as active, 
    sum(item_amt)::float as act_sales,
    sum(item_spread_discount_amt)::float as act_discount,
    count(distinct b.order_num)::int as act_trans,
    sum(item_quantity_val)::int as act_items

    from contacts a
    left join trans b on a.contact_key = b.contact_key
    join segment using (campaign_name, treatment_name, segment_name, control)


    group by 1,2,3,4,5


    """.format(campaign_name=campaign_name, id_seg=id_seg, start_dt=start_dt, end_dt=end_dt, id_treatment=id_treatment)

    return query

def norm_data(campaign_name, norm_start, norm_end, id_treatment='%', id_seg ='%'):
    
    query = """
    with contacts as( 
    select distinct campaign_name, treatment_name, segment_name, 
    case when control_group_flag='Y' 
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    or segment_name like '%Control%'
    or campaign_name like '%CT%'
    
    then 'Y' else 'N' end as control,
    contact_key, campaign_start_dt, campaign_end_dt

    from rawdata.b_camp_offer_rk a join rawdata.b_camp_contact_rk b using (camp_offer_key)

    where campaign_name like '{campaign_name}'
    and {id_seg}
    and {id_treatment}
    ),

    segment as(
    select distinct campaign_name, treatment_name, segment_name, 
    case when control_group_flag='Y' 
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    or segment_name like '%Control%'
    or campaign_name like '%CT%'
      
    then 'Y' else 'N' end as control,
    count(distinct contact_key) as contacted 

    from rawdata.b_camp_offer_rk join rawdata.b_camp_contact_rk using (camp_offer_key)

    where campaign_name like '{campaign_name}'
    and {id_seg}
    and {id_treatment}

    group by 1,2,3,4
    ),
 
    trans as (
    select * from rawdata.b_transaction_rk
    where transaction_dt_key >= {norm_start} and transaction_dt_key <= {norm_end}
    and contact_key > 0
    and transaction_type_name = 'Item'
    )

    select campaign_name, treatment_name, segment_name, control, contacted,
    count(distinct b.contact_key)::int as active, 
    sum(item_amt)::float as act_sales,
    sum(item_spread_discount_amt)::float as act_discount,
    count(distinct b.order_num)::int as act_trans,
    sum(item_quantity_val)::int as act_items

    from contacts a
    left join trans b on a.contact_key = b.contact_key
    join segment using (campaign_name, treatment_name, segment_name, control)


    group by 1,2,3,4,5


    """.format(campaign_name=campaign_name, id_seg=id_seg, norm_start=norm_start, norm_end=norm_end, id_treatment=id_treatment)

    return query


def acv_check(campaign_name, pre_start_dt, pre_end_dt, id_seg):

    query = """
    with contacts as (
    select distinct campaign_name, treatment_name, segment_name,
    case when control_group_flag='Y' 
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    then 'Y' else 'N' end as control,
    contact_key, campaign_start_dt, campaign_end_dt

    from rawdata.b_camp_offer_rk a join rawdata.b_camp_contact_rk b using (camp_offer_key)

    where campaign_name like '{campaign_name}' --Identify the campaign 
    and {id_seg}  --Secondary ID for campaign; 
    )


    select campaign_name, treatment_name, control,--contact_key,
    count(distinct order_num) as trans, 
    --sum(item_quantity_val) as units, 
    --sum(item_amt)::float as sales--, 
    --transaction_dt_KEY
    sum(sale_amt)::float as sales,
    sum(sale_amt)/count(distinct contact_key)::float as ACV,
    sum(sale_amt)/count(distinct order_num)::float as ATV,
    count(distinct contact_key) as universe,
    count(distinct order_num)/count(distinct contact_key)::float as ATF
    


    from rawdata.b_transaction_rk join contacts using (contact_key)
    where transaction_dt_key >= {pre_start_dt} and transaction_dt_key <= {pre_end_dt} 
    and contact_key > 0
    and transaction_type_name = 'Sale'
    and sale_amt>0
    --and treatment_name not like '%Dummy%'
    group by 1,2,3


    """.format(campaign_name=campaign_name, pre_start_dt=pre_start_dt, pre_end_dt=pre_end_dt, id_seg=id_seg)

    return query


def rfm_check(campaign_name, seg_start_dt, id_treatment):
    query = """
    with contacts as (
    select distinct campaign_name, treatment_name, segment_name,
    case when control_group_flag='Y' 
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    then 'Y' else 'N' end as control,
    contact_key, campaign_start_dt, campaign_end_dt

    from rawdata.b_camp_offer_rk a join rawdata.b_camp_contact_rk b using (camp_offer_key)

    where campaign_name like '{campaign_name}%'
    and treatment_name like '{id_treatment}%' 
    

    ),


    seg as (
    select distinct contact_key, seg_num
    from rawdata.seg_rfm_rk
    join (
    select contact_key,max(seg_start_dt) as seg_start_dt
    from rawdata.seg_rfm_rk
    WHERE SEG_START_DT <= '{seg_start_dt}' --YYYY-MM-DD
    group by 1
    ) using (contact_key, seg_start_dt))


    select count(distinct contact_key)::int, seg_num, a.control
    from contacts a 
    left join rawdata.seg_rfm_rk b using (contact_key)
    --where a.control = 'N' --change to N for test 
    where seg_active_flag = 'Y'
    and treatment_name not like '%Dummy%' 
    group by 2,3

    """.format(campaign_name=campaign_name, seg_start_dt=seg_start_dt, c_code=c_code,id_treatment=id_treatment)

    return query


def camp_transactions(campaign_name, start_dt, end_dt):
        
    query="""
    with contacts as (
    select distinct campaign_name, treatment_name, segment_name,
    case when control_group_flag='Y'
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    then 'Y' else 'N' end as control,
    contact_key, campaign_start_dt, campaign_end_dt

    from rawdata.b_camp_offer_rk a join rawdata.b_camp_contact_rk b using (camp_offer_key)

    where campaign_name like '{campaign_name}' --Identify the campaign name
    )

    select  control, 
    count(distinct order_num) as trans, 
    sum(item_quantity_val) as units, 
    sum(item_amt) as sales,
    transaction_Dt_key
    from rawdata.b_transaction_rk

    join contacts using (contact_key)
    where transaction_dt_key >= {start_dt} and transaction_dt_key <= {end_dt} --Select campaign period 
    and contact_key > 0
    and transaction_type_name = 'Item'
    group by 1,5
    """.format(campaign_name=campaign_name, start_dt=start_dt, end_dt=end_dt)

    return query

def sales_rfm_incremental(campaign_name, start_dt, end_dt, rfm_dt, id_seg, id_treatment):
    
    # from datetime import date
    # from dateutil.relativedelta import relativedelta
    # from datetime import datetime

    # date_format = '%Y%m%d'
    # rfm_date_format = '%Y-%m-%d'

    # a = datetime.strptime(str(start_dt), date_format)
    
    # b = a - relativedelta(days=1)


    query = """ with contacts as (
    select distinct campaign_name, treatment_name, segment_name, 
    case when segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    or segment_name like '%Control%'
    or campaign_name like '%CT%'

    then 'Y' else 'N' end as control,
    contact_key, seg_num 

    from rawdata.b_camp_offer_rk join rawdata.b_camp_contact_rk using (camp_offer_key)
    join (select distinct contact_key, seg_num
    from rawdata.seg_rfm_rk
    join (
    select contact_key,max(seg_start_dt) as seg_start_dt
    from rawdata.seg_rfm_rk
    WHERE SEG_START_DT <= '2017-11-26' --YYYY-MM-DD
    group by 1
    ) using (contact_key, seg_start_dt)) 

    using (Contact_key)

    where campaign_name like '{campaign_name}'
    and {id_seg}
    and {id_treatment}

    ),



    trans as (
    select distinct contact_key, item_spread_discount_amt, item_amt,order_num,  item_quantity_val, seg_num from rawdata.b_transaction_rk
    join (select distinct contact_key, seg_num
    from rawdata.seg_rfm_rk
    join (
    select contact_key,max(seg_start_dt) as seg_start_dt
    from rawdata.seg_rfm_rk
    WHERE SEG_START_DT <= {rfm_dt} --YYYY-MM-DD
    group by 1
    ) using (contact_key, seg_start_dt)) 

    using (Contact_key)
    where transaction_dt_key >= {start_dt} and transaction_dt_key <= {end_dt}
    and contact_key > 0
    and transaction_type_name = 'Item'
    )


    select DISTINCT campaign_name, treatment_name, segment_name, control, a.seg_num,
    count(distinct a.contact_key)::int as contacted,
    count(distinct b.contact_key)::int as active, 
    sum(item_amt)::float as act_sales,
    sum(item_spread_discount_amt)::float as act_discount,
    count(distinct b.order_num)::int as act_trans,
    sum(item_quantity_val)::int as act_items


    from contacts a
    left join trans b on a.contact_key = b.contact_key
    --join counts using (campaign_name, treatment_name, segment_name, control)

    group by 1,2,3,4,5;""".format(campaign_name=campaign_name, rfm_dt=rfm_dt, start_dt=start_dt, end_dt=end_dt, id_seg=id_seg, id_treatment=id_treatment)

    return query

def sales_by_rfm(campaign_name, start_dt, end_dt, seg_start, id_treatment='%', id_seg ='%'):
    
    query = """
    with contacts as( 
    select distinct campaign_name, treatment_name, segment_name, seg_num,

    case when control_group_flag='Y' 
    or segment_name like '%CTRL%' 
    or segment_name like '%_CT%' 
    or segment_name like '%CONTRL%'
    or segment_name like '%CONTROL%'
    or segment_name like '%_CT_%'
    or segment_name like '%control%'
    or segment_name like '%Control%'
    or campaign_name like '%CT%'
    
    then 'Y' else 'N' end as control,

    contact_key, campaign_start_dt, campaign_end_dt

    from rawdata.b_camp_offer_rk a join rawdata.b_camp_contact_rk b using (camp_offer_key)

    
    join (select distinct contact_key, seg_num
    from rawdata.seg_rfm_rk
    join (
    select contact_key,max(seg_start_dt) as seg_start_dt
    from rawdata.seg_rfm_rk
    WHERE SEG_START_DT <= '{seg_start}' --YYYY-MM-DD
    group by 1
    ) using (contact_key, seg_start_dt)) 

    using (Contact_key)


    where campaign_name like '{campaign_name}'
    and {id_seg}
    and {id_treatment}
    ), 


    trans as (
    select * from rawdata.b_transaction_rk
    where transaction_dt_key >= {start_dt} and transaction_dt_key <= {end_dt}
    and contact_key > 0
    and transaction_type_name = 'Item'
    )

    select campaign_name, treatment_name, segment_name, control, seg_num,
    count(distinct b.contact_key)::int as active, 
    sum(item_amt)::float as act_sales,
    sum(item_spread_discount_amt)::float as act_discount,
    count(distinct b.order_num)::int as act_trans,
    sum(item_quantity_val)::int as act_items

    from contacts a
    left join trans b on a.contact_key = b.contact_key
    --join segment using (campaign_name, treatment_name, segment_name, control)

    where control = 'N'


    group by 1,2,3,4,5


    """.format(campaign_name=campaign_name, id_seg=id_seg, start_dt=start_dt, end_dt=end_dt, seg_start=seg_start, id_treatment=id_treatment)

    return query