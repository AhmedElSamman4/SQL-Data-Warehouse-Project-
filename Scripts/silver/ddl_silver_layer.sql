use FDWH
/*
===============================================================
==>DDl script: create tables silver 
	==>purpose:
		-this script creates tables of silver layer , 
		-dropping existing tables if they already exist 
===============================================================
*/
if object_id('silver.crm_cust_info','U') is not null 
			drop table silver.crm_cust_info

	create table silver.crm_cust_info(
		cst_id int, 
		cst_key nvarchar(40),
		cst_firstname nvarchar(50),
		cst_lastname nvarchar(50),
		cst_marital_status nvarchar(40),
		cst_gnder nvarchar(50),
		cst_create_date datetime
	 )
	
if object_id('silver.crm_prd_info','U') is not null /*checking if the table already exist*/
	/* 'U' -->(user table) , 'V' --->(view) , 'P' ---->(stored preocedure) */
		drop table silver.crm_prd_info

	create table silver.crm_prd_info(
	prd_id int, 
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost numeric,
	prd_line nvarchar(50),
	prd_start_dt datetime, 
	prd_end_dt datetime
	)

if object_id('silver.crm_sales_details','U') is not null 
		drop table silver.crm_sales_details

	create table silver.crm_sales_details(
	sls_ord_num nvarchar(50) , 
	sls_prd_key nvarchar(50),
	sls_cust_id int ,
	sls_order_dt date, 
	sls_ship_dt date, 
	sls_due_dt date , 
	sls_sales int ,
	sls_quantity int , 
	sls_price int
	)

if object_id('silver.erp_cust_az12','U') is not null 
	drop table silver.erp_cust_az12
	create table silver.erp_cust_az12(
	cid nvarchar(50) ,
	bdate date , 
	gen nvarchar(50)
	)

if object_id('silver.erp_loc_a101','U') is not null 
	drop table silver.erp_loc_a101
	create table silver.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50)
	)

if object_id('silver.erp_px_cat_g1v2','U') is not null 
	drop table silver.erp_px_cat_g1v2
	create table silver.erp_px_cat_g1v2(
	id nvarchar(50) , 
	cat nvarchar(50) , 
	subcat nvarchar(50),
	mantenance nvarchar(50)
	)
