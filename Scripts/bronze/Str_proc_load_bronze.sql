/*
==>stored procedure : load bronze layer (source -->bronze)

==>purpose:
    -this script load data into bronze schema from exteral csv files

  ==>it perforems :
    - truncate the bronze tables before loading data
    -uses "bulk insert" command to load data from external csv files
*/

exec bronze.load_bronze
create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime , @end_time datetime;
	declare @batch_start_time datetime, @batch_end_time datetime;
	set @batch_start_time= GETDATE()
	begin try
		print '####################'
		print 'loading bronze layer'
		print '####################'


		print '####################'
		print 'loading CRM Tables'
		print '####################'

		--===================================
		--creating the crm_cust_info table
		--====================================
		set @start_time = getdate()
		if object_id('bronze.crm_cust_info','U') is not null 
			drop table bronze.crm_cust_info

		create table bronze.crm_cust_info(
		cst_id int, 
		cst_key nvarchar(40),
		cst_firstname nvarchar(50),
		cst_lastname nvarchar(50),
		cst_material_status nvarchar(40),
		cst_gnder nvarchar(50),
		cst_create_date datetime
		)
		set @end_time = getdate()
		print 'the duration is ' + cast(datediff(second, @start_time , @end_time)as nvarchar)+'second'

		--===================================
		--creating the crm_prd_info table
		--====================================
		set @start_time = getdate()
		if object_id('bronze.crm_prd_info','U') is not null /*checking if the table already exist*/
			drop table bronze.crm_prd_info

		create table bronze.crm_prd_info(
		prd_id int, 
		prd_key nvarchar(50),
		prd_nm nvarchar(50),
		prd_cost numeric,
		prd_line nvarchar(50),
		prd_start_dt datetime, 
		prd_end_dt datetime
		)
		set @end_time= getdate()
		print 'the duration is ' + cast(datediff(second, @start_time , @end_time)as nvarchar)+'second'
		--===================================
		--creating the crm_sales_details table
		--====================================
		set @start_time=GETDATE()
		if object_id('bronze.crm_sales_details','U') is not null 
			drop table bronze.crm_sales_details

		create table bronze.crm_sales_details(
		sls_ord_num nvarchar(50) , 
		sls_prd_key nvarchar(50),
		sls_cust_id int ,
		sls_order_dt int, 
		sls_ship_dt int, 
		sls_due_dt int , 
		sls_sales int ,
		sls_quantity int , 
		sls_price int
		)
		set @end_time=GETDATE()
	    print 'the duration is ' + cast(datediff(second, @start_time , @end_time)as nvarchar)+'second'
		--===================================
		--creating the erp_cust_az12 table
		--====================================
		set @start_time=GETDATE()
		if object_id('bronze.erp_cust_az12','U') is not null 
			drop table bronze.erp_cust_az12
		create table bronze.erp_cust_az12(
		cid nvarchar(50) ,
		bdate date , 
		gen nvarchar(50)
		)
		set @end_time=GETDATE()
		print 'the duration is ' + cast(datediff(second, @start_time , @end_time)as nvarchar)+'second'
		--===================================
		--creating the erp_loc_a101 table
		--====================================
		set @start_time = getdate()
		if object_id('bronze.erp_loc_a101','U') is not null 
			drop table bronze.erp_loc_a101
		create table bronze.erp_loc_a101(
		cid nvarchar(50),
		cntry nvarchar(50)
		)
		set @end_time = getdate()
		print 'the duration is ' + cast(datediff(second, @start_time , @end_time)as nvarchar)+'second'
		--===================================
		--creating the erp_px_cat_g1v2 table
		--====================================
		set @start_time = GETDATE()
		if object_id('bronze.erp_px_cat_g1v2','U') is not null 
			drop table bronze.erp_px_cat_g1v2
		create table bronze.erp_px_cat_g1v2(
		id nvarchar(50) , 
		cat nvarchar(50) , 
		subcat nvarchar(50),
		mantenance nvarchar(50)
		)
		set @end_time = getdate()
		print 'the duration is ' + cast(datediff(second, @start_time , @end_time)as nvarchar)+'second'
		--================================== 
		 --full load
		--==================================
		Truncate table bronze.crm_cust_info /* to ensure that the table is already empty */
		Bulk insert bronze.crm_cust_info
		from 'C:\Baraa_Data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow=2,
			fieldterminator=',',
			tablock 
		)

		--===============================================
		Truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from 'C:\Baraa_Data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow= 2,
			fieldterminator = ',',
			Tablock
		)

		--===================================================
		Truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\Baraa_Data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow= 2,
			fieldterminator = ',',
			tablock
		)

		--===============================================
		Truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'C:\Baraa_Data\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow= 2,
			fieldterminator = ',',
			tablock
		)

		--===============================================
		Truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'C:\Baraa_Data\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow= 2,
			fieldterminator = ',',
			tablock
		)

		--===============================================
		Truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Baraa_Data\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow= 2,
			fieldterminator = ',',
			tablock
		)
		set @batch_end_time= GETDATE()
		print '================================='
		print 'loading the broze layer is done'
        print CONCAT('The total time of loading the bronze layer is ',datediff(second,@batch_start_time,@batch_end_time),' seconds');
		print '=================================='
	end try 
	begin catch
		print 'Error'
		print 'the error message ' + Error_message()
		print 'the error num' + cast(error_number() as varchar)
		print '#########################################'
	end catch

end

	
