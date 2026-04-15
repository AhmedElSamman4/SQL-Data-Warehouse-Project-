create or alter view gold.dim_customer as
	select 
		row_number()over(order by cst_id ) as customer_number,
		cinf.cst_id as customer_id, 
		cinf.cst_key as customer_key,
		cinf.cst_firstname as firstname,
		cinf.cst_lastname as lastname, 
		la.cntry as country,
		ca.bdate as birth_date,
			case when cinf.cst_gnder!='n\a' then cst_gnder
			else coalesce(ca.gen,'n\a') 
		end as gender,
		cinf.cst_marital_status marital_status,
		cinf.cst_create_date as create_date
	from silver.crm_cust_info cinf
	left join silver.erp_cust_az12 ca
		on ca.cid=cinf.cst_key
	left join silver.erp_loc_a101 la
		on cinf.cst_key=la.cid


--==============================================================
create or alter view gold.dim_products as
	select 
		row_number()over(order by cp.prd_start_dt,cp.prd_key) as product_key,
		cp.prd_id as product_id,
		cp.prd_nm as product_name,
		cp.cat_id as category_id ,
		cp.prd_key as product_number,
		cp.prd_cost as product_cost,
		cp.prd_line as product_line,
		cp.prd_start_dt as start_date,
		pg.subcat as subcategory,
		pg.mantenance
	from silver.crm_prd_info cp
	left join silver.erp_px_cat_g1v2 pg
		on cp.cat_id=pg.id
	where cp.prd_end_dt is NULL

--=================================================
create or alter view gold.fact_sales as
	select	
		sd.sls_ord_num as order_number,
		dc.customer_number,--suriget key of dim customer
		dp.product_key,--suriget key od dim product
		sd.sls_quantity as quantity,
		sd.sls_sales as sales_amount,
		sd.sls_order_dt as order_date,
		sd.sls_ship_dt shipping_date,
		sd.sls_due_dt as due_date
	from silver.crm_sales_details sd
	left join  gold.dim_customer dc
	 on sd.sls_cust_id = dc.customer_id
	left join gold.dim_products dp
	 on sd.sls_prd_key=dp.product_number
