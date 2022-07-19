use MX_DWH
Go

if object_id ('etl.load_ods_opr_budget', 'P') is not null
	drop procedure etl.load_ods_opr_budget
GO

create procedure etl.load_ods_opr_budget (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	24/06/2022
	-- Description:	Load OPR Budget
	-- =============================================
	-- 24/06/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(255), @src varchar(255), @sql varchar(2000),
			@srv varchar(255), @proceso varchar(30), @periodo varchar(6),
			@año varchar(6)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))
		create table #budget (
			Mes	varchar(30) COLLATE database_default,
			Tipo varchar(30) COLLATE database_default,
			Empresa	varchar(30) COLLATE database_default,
			MainAccount	varchar(30) COLLATE database_default,
			CostCenter	varchar(30) COLLATE database_default,
			SubServiceLine	varchar(80) COLLATE database_default,
			Cliente varchar(80) COLLATE database_default,
			Debito float,
			Credito float,
			Importe float )

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @periodo = year(getdate()) * 100 +  month(getdate()) 
		set @msg = 'Periodo: ' + @periodo; exec etl.prc_Logging @idBatch, @msg

		if @idBatch > 0
			set @proceso = etl.fn_getProcesoBatch(@idBatch)
		else 
			set @proceso = 'ODS_OPR_BUDGET'

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'OPR\Budget\'
		set @path = @dir + 'Budget*.xlsx'
		set @cmd = 'dir "' + @path + '" /b'
		set @srv = etl.fn_getParametroGral('EXCEL-SRV')

		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		-- Lineas_EstadoGestion.xls
		delete from #tmpArchivo
		where archivo is null or archivo not like '%.xlsx'

		-- Validar Si hay Archivos
		if not exists( select 1 from #tmpArchivo)
			begin
				set @msg = 'No existen archivo para procesar, el proceso no continua.'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				return 0
			end

		-------------------------------------------
		-- PROCESAR ARCHIVOS 
		-------------------------------------------
		

		-- Control Carga Archivos
		delete stg.ST_CONTROL_ARCHIVOS 
		where proceso = @proceso
			and periodo = @periodo
			and procesado <> 'S'

		update stg.ST_CONTROL_ARCHIVOS 
		set procesado = 'X'
		where proceso = @proceso
			and procesado = 'N'


		declare lc_cursor cursor for
		select archivo,
			@dir + archivo as path,
			substring(archivo,8,4) as año
		from #tmpArchivo
		order by 1
		
		open lc_cursor
		fetch next from lc_cursor into @archivo, @path, @año
		while @@fetch_status = 0
			begin
				set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path

				set @sql = '
					insert into #budget (Mes, Tipo, Empresa, MainAccount, CostCenter, 
						SubServiceLine, Cliente, Debito, Credito, Importe) 
					SELECT concat('+ @año + ', etl.fn_lpad(Mes, 2, ''0'')) as Mes,
						etl.fn_trim(Tipo) as Tipo,
						etl.fn_trim(GEM) as Empresa,
						etl.fn_trim(MainAccount) as MainAccount,
						etl.fn_trim(LocalCostCenter) as CostCenter,
						etl.fn_trim(LocalSubServiceLine) as SubServiceLine,
						etl.fn_trim(ClientOrganizationUnit) as Cliente,
						isnull([Débito],0) as Debito,
						isnull([Crédito],0) as Credito,
						isnull([Saldo de cierre],0) as Importe
					FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Info$]'') X
					WHERE 1=1
						and GEM is not null'
					

				begin try
					exec (@sql)
					set @cant = @@rowCount
					set @msg = 'Load Archivo: ' + @archivo + ' - Ins: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg
				end try
				begin catch
					set @cant = 0
					set @msg = 'Error al Cargar Archivo: ' + @archivo + ' - Msg: ' + isnull(ERROR_MESSAGE(),'<null>')
					exec etl.prc_Logging @idBatch, @msg
				end catch

				if @cant > 0 
					begin
						insert into stg.ST_CONTROL_ARCHIVOS (
							periodo, proceso, archivo, path, registros, procesado)
						values ( @periodo, @proceso, @path, @dir, @cant, 'N')
					end

				-- next
				fetch next from lc_cursor into @archivo, @path, @año
			end
		close lc_cursor
		deallocate lc_cursor

		begin transaction

			truncate table stg.ST_ODS_OPR_BUDGET

			insert into stg.ST_ODS_OPR_BUDGET (periodo, tipo,
				cod_empresa, cod_sucursal, cod_cliente,
				cod_servicio, cod_cuenta, debito, credito, importe) 
			select Mes as Periodo, 
				isnull(Tipo, 'Budget') as tipo,
				isnull(Empresa, 'NA') as cod_empresa,
				replace(CostCenter,'MX_','') as cod_sucursal, 
				isnull(Cliente,'NA') as cod_cliente, 
				isnull(SubServiceLine,'NA') as cod_sevicio, 
				isnull(MainAccount,'NA') as cod_cuenta, 
				sum(isnull(Debito,0)) as debito,
				sum(isnull(Credito,0)) as credito,
				sum(isnull(Importe,0)) as importe
			from #budget
			group by Mes, 
				isnull(Tipo, 'Budget'),
				isnull(Empresa, 'NA'),
				replace(CostCenter,'MX_',''), 
				isnull(Cliente,'NA'), 
				isnull(SubServiceLine,'NA'), 
				isnull(MainAccount,'NA')

			set @cant = @@rowCount
			set @msg = 'Insertados: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		commit transaction


		-- Ok
		if @cant > 0
			return 1
		else
			return 0

	end try

	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction
			exec etl.prc_log_error_sp 'MAPPING', @idBatch

		-- Error
		return -1

	end catch
end
GO
-- exec etl.load_ods_opr_budget 0