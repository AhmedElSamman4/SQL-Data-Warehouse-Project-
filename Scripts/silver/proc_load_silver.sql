/*
========================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
========================================================================
*/
create or alter procedure silver.load_silver as 
begin
	print 'insert into silver.crm_cust_info'
	print '============================================='
	if object_id('silver.crm_cust_info','U') is not null 
		truncate table silver.crm_cust_info
	insert into silver.crm_cust_info(
	cst_id ,
	cst_key ,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gnder,cst_create_date)
	select 
	cst_id ,
	cst_key ,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	/* full Null */
	case when upper(cst_marital_status) = 'S' then 'Single'
		 when upper(cst_marital_status) = 'M' then 'Married'
		 else 'n\a'
	end cst_marital_status,
	/*full Null*/
	case when upper(cst_gnder)= 'M' then 'Male'
		 when upper(cst_gnder)= 'F' then 'Female'
		 else 'n\a'
	end cst_gnder,
	cst_create_date
	/*removing duplicate values from primary key*/
	from( select *,
	ROW_NUMBER()over(partition by cst_id order by cst_create_date DESC) as flage_last
	from bronze.crm_cust_info
	where cst_id is not null) t
	where flage_last =1 
	--##############################################################
	--##############################################################
	print '============================================'
	print 'insert inot silver.crm_prd_info'
	--inster into table silver.crm_prd_info
	if object_id('silver.crm_prd_info','U') is not null 
		truncate table silver.crm_prd_info
	insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	select 
	prd_id , 
	replace(substring(prd_key , 1, 5),'-','_') as cat_id,
	substring(prd_key, 7,len(prd_key))AS prd_key,
	prd_nm, 
	isnull(prd_cost,0) as prd_cost,
	case when upper(trim(prd_line)) = 'M' THEN 'Mountain'
		 when upper(trim(prd_line)) = 'R' THEN 'Road'
		 when upper(trim(prd_line)) = 'S' THEN 'Other sales'
		 when upper(trim(prd_line)) = 'T' THEN 'Touring'
		 else 'n\a'
	end as prd_line,
	/* case  upper(trim(prd_line)) 
			when 'M' THEN 'Mountain'
			when 'R' THEN 'Road'
			when 'S' THEN 'Other sales'
			when 'T' THEN 'Touring'
			else 'n\a'
	end as prd_line,
	*/
	prd_start_dt,
	lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_date
	from bronze.crm_prd_info

	/*
	select * from bronze.erp_px_cat_g1v2

	/*check if the product name have a unneeded spaces*/
	select prd_nm 
	from bronze.crm_prd_info
	where prd_nm!=trim(prd_nm)

	/*check if the product cost have a null values or minus values*/
	select prd_cost 
	from bronze.crm_prd_info
	where prd_cost<0 or prd_cost is null

	/*check the distinct values of product line column */
	select distinct prd_line 
	from bronze.crm_prd_info

	/*check the prd_start_dt and prd_end_dt logic*/
	select * 
	from bronze.crm_prd_info 
	where prd_start_dt>prd_end_dt
	/* I found alot of start dates greater than end dates so we can solve this problem 
	 by replacing the end date with the next start date for every product*/
	*/

	--###############################################################
	--###############################################################
	print '==============================================='
	print 'insert into silver.crm_sales_details'

	if object_id('silver.crm_sales_details','U') is not null 
		truncate table silver.crm_sales_details
	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt<=0 or len(sls_order_dt)<8 then NULL
		 else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt,
	case when sls_ship_dt<=0 or len(sls_ship_dt)<8 then NULL
		 else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt,

	case when sls_due_dt<=0 or len(sls_due_dt)<8 then NULL
		 else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt,

	case when sls_sales!=(sls_quantity*sls_price) or sls_sales is NULL or sls_sales <=0
		then sls_quantity*abs(sls_price)
		else sls_sales
	end as sls_sales,

	case when sls_quantity is null or sls_quantity<=0 
		 then sls_sales/nullif(sls_quantity,0)
		 else sls_quantity
	end as sls_quantity,

	case when sls_price is null or sls_price<=0 
		 then sls_sales/nullif(sls_price,0)
		 else sls_price
	end as sls_price
	from bronze.crm_sales_details


	/*since we know that the sls_sales = sls_quantity * sls_price 
	so we have to chect if the sls_sales = sls_quantity * sls_price */

	/*select 
	sls_sales,
	sls_quantity,
	sls_price
	from bronze.crm_sales_details
	where sls_sales != (sls_quantity*sls_price)
	or sls_sales is  null or sls_quantity is  null or sls_price is  null
	or sls_sales <=0 or sls_quantity <=0 or sls_price <=0*/

	--###############################################################
	--###############################################################
	print '==============================================='
	print 'insert into silver.erp_cust_az12'

	if object_id('silver.erp_cust_az12','U') is not null 
		truncate table silver.erp_cust_az12
	insert into silver.erp_cust_az12(cid, bdate, gen)
	select 
	case when cid like 'NAS%' then substring(cid, 4,len(cid))
		 else cid
	end as cid,
	case when bdate > getdate() then NULL 
		 else bdate
	end as bdate, 
	case when trim(upper(gen)) in ('F','Female') then 'Female'
		 when trim(upper(gen)) in ('M','Male') then 'Male'
		 else 'n\a'
	end as gen
	from bronze.erp_cust_az12

	--###############################################################
	--###############################################################
	print '==============================================='
	print 'insert into silver.erp_loc_a101'

	if object_id('silver.erp_loc_a101','U') is not null 
		truncate table silver.erp_loc_a101
	insert into silver.erp_loc_a101 ( cid, cntry)
	select 
	replace(cid,'-','') as cid,
	case 
		when trim(cntry) in ('US','USA') then 'United States'
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) = '' or cntry is NULL then 'n\a'
		else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101

	--###############################################################
	--###############################################################
	print '==============================================='
	print 'insert into silver.erp_px_cat_g1v2'

	if object_id('silver.erp_px_cat_g1v2','U') is not null 
		truncate table silver.erp_px_cat_g1v2
	insert into silver.erp_px_cat_g1v2
	(id, cat, subcat,mantenance)
	select 
	id,cat, subcat,mantenance
	from bronze.erp_px_cat_g1v2
end
