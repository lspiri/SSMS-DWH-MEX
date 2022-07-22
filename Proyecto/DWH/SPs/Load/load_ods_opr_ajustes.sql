use MX_DWH
Go

if object_id ('etl.load_ods_opr_ajustes', 'P') is not null
	drop procedure etl.load_ods_opr_ajustes
GO

create procedure etl.load_ods_opr_ajustes (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	30/04/2022
	-- Description:	Load Ajustes en OPR
	-- =============================================
	-- 02/06/2022 - Version Inicial
	-- 22/07/2022 - LS - Ajuste al Tomar Periodo en Archivo

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(255), @src varchar(255), @sql varchar(2000),
			@srv varchar(255), @proceso varchar(30), @periodo varchar(6),
			@periodo_archivo varchar(6), @cant_total numeric(10)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))
		create table #ajustes_eg (
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

		create table #ajustes_cc (
			Mes	varchar(30) COLLATE database_default,
			Cuenta	varchar(30) COLLATE database_default,
			CC	varchar(30) COLLATE database_default
			)

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @periodo = year(getdate()) * 100 +  month(getdate()) 
		set @msg = 'Periodo: ' + @periodo; exec etl.prc_Logging @idBatch, @msg

		if @idBatch > 0
			set @proceso = etl.fn_getProcesoBatch(@idBatch)
		else 
			set @proceso = 'ODS_OPR_AJUSTES'

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'OPR\Ajustes\'
		set @path = @dir + 'Ajuste*.xlsx'
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
		begin transaction

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
				substring(archivo,12,6) as periodo
			from #tmpArchivo
			order by 1

			
			open lc_cursor
			fetch next from lc_cursor into @archivo, @path, @periodo_archivo
			while @@fetch_status = 0
				begin
					set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path

					if @archivo like 'Ajustes_EG%'
						begin

							set @sql = '
								insert into #ajustes_eg (Mes, Tipo, Empresa, MainAccount, CostCenter, 
									SubServiceLine, Cliente, Debito, Credito, Importe) 
								SELECT '+ @periodo_archivo +' as Mes,
									etl.fn_trim(Tipo) as Tipo,
									etl.fn_trim(GEM) as Empresa,
									etl.fn_trim(MainAccount) as MainAccount,
									etl.fn_trim(LocalCostCenter) as CostCenter,
									etl.fn_trim(LocalSubServiceLine) as SubServiceLine,
									etl.fn_trim(ClientOrganizationUnit) as Cliente,
									isnull([Débito],0) as Debito,
									isnull([Crédito],0) as Credito,
									isnull([DEL MES],0) as Importe
								FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Info$]'') X
								WHERE 1=1
									and GEM is not null
									and cast(Mes as int) = '''+ cast(cast(right(@periodo_archivo,2) as int) as varchar) +''''
						end

					if @archivo like 'Ajustes_CC%'
						begin

							set @sql = '
								insert into #ajustes_cc (Mes, Cuenta, CC) 
								SELECT '+ @periodo_archivo +' as Mes,
									etl.fn_trim(Cuenta) as Cuenta,
									etl.fn_trim(CC) as CC
								FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Info$]'') X
								WHERE 1=1
									and CC is not null'
						end

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
					fetch next from lc_cursor into @archivo, @path, @periodo_archivo
				end
			close lc_cursor
			deallocate lc_cursor

			set @cant_total = 0
			truncate table stg.ST_ODS_OPR_AJUSTES_EG
			truncate table stg.ST_ODS_OPR_AJUSTES_CC

			insert into stg.ST_ODS_OPR_AJUSTES_EG (periodo, tipo,
				cod_empresa, cod_sucursal, cod_cliente,
				cod_servicio, cod_cuenta, debito, credito, importe) 
			select Mes as Periodo, 
				isnull(Tipo, 'Ajuste_EG') as tipo,
				isnull(Empresa, 'NA') as cod_empresa,
				replace(CostCenter,'MX_','') as cod_sucursal, 
				isnull(Cliente,'NA') as cod_cliente, 
				isnull(SubServiceLine,'NA') as cod_sevicio, 
				isnull(MainAccount,'NA') as cod_cuenta, 
				sum(isnull(Debito,0)) as debito,
				sum(isnull(Credito,0)) as credito,
				sum(isnull(Importe,0)) as importe
			from #ajustes_eg
			group by Mes, 
				isnull(Tipo, 'Ajuste_EG'),
				isnull(Empresa, 'NA'),
				replace(CostCenter,'MX_',''), 
				isnull(Cliente,'NA'), 
				isnull(SubServiceLine,'NA'), 
				isnull(MainAccount,'NA')

			set @cant = @@rowCount
			set @cant_total = @cant_total + isnull(@cant,0)
			set @msg = 'Ins. (Ajustes EG): ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

			insert into stg.ST_ODS_OPR_AJUSTES_CC (periodo,
				cod_cuenta, cod_sucursal) 
			select Mes as Periodo, 
				isnull(Cuenta,'NA') as cod_cuenta, 
				max(replace(CC,'MX_','')) as cod_sucursal
			from #ajustes_cc
			group by Mes, 
				isnull(Cuenta,'NA')

			set @cant = @@rowCount
			set @cant_total = @cant_total + isnull(@cant,0)
			set @msg = 'Ins. (Ajustes CC): ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		commit transaction


		-- Ok
		if @cant_total > 0
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
-- exec etl.load_ods_opr_ajustes 0