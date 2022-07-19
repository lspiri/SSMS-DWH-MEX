use MX_DWH
Go

if object_id ('etl.load_ods_opr_dso', 'P') is not null
	drop procedure etl.load_ods_opr_dso
GO

create procedure etl.load_ods_opr_dso (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	31/05/2022
	-- Description:	Load DSOs para OPR
	-- =============================================
	-- 20/06/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(255), @src varchar(255), @sql varchar(2000),
			@srv varchar(255), @proceso varchar(30), @periodo varchar(6),
			@periodo_archivo varchar(6), @nroEmp varchar(2),
			@CodEmpresa varchar(30)

		declare @Emp_01 varchar(30), @Emp_02 varchar(30), @Emp_03 varchar(30),
			@Emp_04 varchar(30), @Emp_05 varchar(30), @Emp_06 varchar(30),
			@Emp_07 varchar(30), @Emp_08 varchar(30), @Emp_09 varchar(30)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))
		create table #dso (
			Mes	varchar(30) COLLATE database_default,
			CC varchar(30) COLLATE database_default,
			Emp_1181 float,
			Emp_1184 float,
			Emp_1185 float,
			Emp_1188 float,
			Emp_2012 float,
			Emp_2013 float,
			Emp_2015 float,
			Emp_2016 float,
			Emp_2018 float
		)

		create table #opr_dso (
			Periodo	varchar(30),
			Cod_Sucursal varchar(30),	
			Cod_Empresa	varchar(30),
			Dso numeric(18,3)
		)

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @periodo = year(getdate()) * 100 +  month(getdate()) 
		set @msg = 'Periodo: ' + @periodo; exec etl.prc_Logging @idBatch, @msg

		if @idBatch > 0
			set @proceso = etl.fn_getProcesoBatch(@idBatch)
		else 
			set @proceso = 'ODS_OPR_DSO'

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'OPR\DSO\'
		set @path = @dir + 'DSO*.xlsx'
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
				right(replace(archivo,'.xlsx',''),6) as periodo
			from #tmpArchivo
			order by 1
		
			open lc_cursor
			fetch next from lc_cursor into @archivo, @path, @periodo_archivo
			while @@fetch_status = 0
				begin
					set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path
					set @sql = '
						insert into #dso (Mes, CC, Emp_1181, Emp_1184, Emp_1185, Emp_1188, 
							Emp_2012, Emp_2013, Emp_2015, Emp_2016, Emp_2018) 
						SELECT case when mes like ''%mes%'' then ''MES'' else '''+ @periodo_archivo +''' end as Mes,
							etl.fn_trim(CC) as CC,
							[1181] as Emp_1181,
							[1184] as Emp_1184,
							[1185] as Emp_1185,
							[1188] as Emp_1188,
							[2012] as Emp_2012,
							[2013] as Emp_2013,
							[2015] as Emp_2015,
							[2016] as Emp_2016,
							[2018] as Emp_2018
						FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Info$]'') X
						WHERE 1=1
							and CC is not null
							and (case when Mes LIKE ''%mes%'' then 1
									when isNumeric(Mes) = 1 then
										case when cast(cast(Mes as int) as varchar) = '''+ cast(cast(right(@periodo_archivo,2) as int) as varchar) +''' then 
											1
										else
											0
										end
									else
										0
									end) = 1'

					--print @sql
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

			-- select * from #dso where cc = 'MX_101042' and mes = '202201'
			
			-- Headers
			select @Emp_01 = '1181', @Emp_02 = '1184', @Emp_03 = '1185',
				@Emp_04 = '1188', @Emp_05 = '2012', @Emp_06 = '2013',
				@Emp_07 = '2015', @Emp_08 = '2016', @Emp_09 = '2018'

			-- Empresas
			declare lc_cursor cursor for
			select right(cast(nro + 100 as varchar),2) as Nroemp from etl.fn_genTabla(1,9,1) e

			open lc_cursor
			fetch next from lc_cursor into @nroEmp
			while @@fetch_status = 0
				begin

					select @CodEmpresa = case @nroEmp
						when '01' then @Emp_01
						when '02' then @Emp_02
						when '03' then @Emp_03
						when '04' then @Emp_04
						when '05' then @Emp_05
						when '06' then @Emp_06
						when '07' then @Emp_07
						when '08' then @Emp_08
						when '09' then @Emp_09
						else 'NA' end
			
					set @sql = 'insert into #opr_dso (Periodo, Cod_Sucursal, Cod_Empresa, Dso)
							select Periodo, Cod_Sucursal, Cod_Empresa, Dso
							from (
								select Mes as Periodo,
									replace(CC,''MX_'','''') as Cod_Sucursal,'
									+ @CodEmpresa + 'as Cod_empresa,
									cast(replace(replace(isnull(Emp_'+@CodEmpresa+',''0''),''-'',''0''),'','',''.'') as numeric(18,3)) as DSO
								from #dso) vw
							where vw.Dso != 0'

					exec (@sql)
					set @msg = 'Empresa: ' + @CodEmpresa + ' - Ins: ' + cast(@@rowCount as varchar); exec etl.prc_Logging @idBatch, @msg
					
					-- next
					fetch next from lc_cursor into @nroEmp
				end
			close lc_cursor
			deallocate lc_cursor

			truncate table stg.ST_ODS_OPR_DSO

			insert into stg.ST_ODS_OPR_DSO (Periodo, Cod_Sucursal, Cod_Empresa, Dso)
			select distinct Periodo, Cod_Sucursal, Cod_Empresa, Dso
			from #opr_dso
			order by 1,2

			set @cant = @@rowCount
			exec etl.prc_Logging @idBatch, '------'
			set @msg = 'Total: ' + @CodEmpresa + ' - Ins: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

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
-- exec etl.load_ods_opr_dso 0