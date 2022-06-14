use MX_DWH
Go

if object_id ('etl.load_ods_deduccion_nomina', 'P') is not null
	drop procedure etl.load_ods_deduccion_nomina
GO

create procedure etl.load_ods_deduccion_nomina (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	19/05/2022
	-- Description:	Load Deduccion Nominas
	-- =============================================
	-- 14/06/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(255), @src varchar(255), @sql varchar(2000),
			@srv varchar(255), @proceso varchar(30), @periodo varchar(6)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))

		create table #SalariosMinimos (
			Num_Cia	int,
			Compañia varchar(255) COLLATE database_default,
			Salario_Minimo float )

		create table #ConceptosPercepcion (
			Concepto varchar(30) COLLATE database_default,	
			Descripcion	varchar(255) COLLATE database_default,
			Suma_Dias varchar(30) COLLATE database_default )

		create table #DeduccionesPrioridad (
			Agrupacion_Paises varchar(30) COLLATE database_default,
			CC_Nomina varchar(30) COLLATE database_default,
			Fin_Validez varchar(30) COLLATE database_default,
			Inicio_Validez varchar(30) COLLATE database_default,
			Nro_Orden numeric(10),
			Concepto_Human varchar(30) COLLATE database_default,
			Texto_CC_Nomina varchar(255) COLLATE database_default,
			Prioridad_Deduc_Moras varchar(30) COLLATE database_default,
			Caracteristica_Demora varchar(30) COLLATE database_default,
			Desc_Aplicacion_Descuento varchar(255) COLLATE database_default)

		create table #ConceptoEmpleados (
			sociedad varchar(30) COLLATE database_default, 
			cia_adecco int, 
			nombre_cia varchar(255) COLLATE database_default, 
			proceso int, 
			descr_proceso varchar(30) COLLATE database_default, 
			tipo_empleado varchar(30) COLLATE database_default, 
			anio_proceso int, 
			num_per_proceso int, 
			fecha_inicio_proceso varchar(30) COLLATE database_default, 
			fecha_fin_proceso varchar(30) COLLATE database_default, 
			mes_contable_proceso int, 
			num_sem_proceso int, 
			perner varchar(30) COLLATE database_default, 
			numero_empleado numeric(20), 
			paterno_empleado varchar(255) COLLATE database_default, 
			materno_empleado varchar(255) COLLATE database_default, 
			nombre_empleado varchar(255) COLLATE database_default, 
			nss varchar(30) COLLATE database_default, 
			area_personal varchar(30) COLLATE database_default, 
			area_personal1 varchar(30) COLLATE database_default, 
			registro_patronal varchar(30) COLLATE database_default, 
			fecha_ingreso varchar(30) COLLATE database_default, 
			fecha_antiguedad varchar(30) COLLATE database_default, 
			division varchar(30) COLLATE database_default, 
			descr_division varchar(255) COLLATE database_default, 
			subdivision varchar(255) COLLATE database_default, 
			descr_subdivision varchar(255) COLLATE database_default, 
			descr_centro_costo varchar(255) COLLATE database_default, 
			centro_costo varchar(30) COLLATE database_default, 
			concepto varchar(30) COLLATE database_default, 
			descr_concepto varchar(255) COLLATE database_default, 
			variable_tiempo int, 
			importe float)

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @periodo = year(getdate()) * 100 +  month(getdate()) 
		set @msg = 'Periodo: ' + @periodo; exec etl.prc_Logging @idBatch, @msg

		if @idBatch > 0
			set @proceso = etl.fn_getProcesoBatch(@idBatch)
		else 
			set @proceso = 'ODS_DEDUCCION_NOMINA'
		

		-- Obtener Archivo De Configuracion
		set @msg = '- Load Archivo Configuracion: '; exec etl.prc_Logging @idBatch, @msg
		begin try

			set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'Deducciones_Nomina\'
			set @path = @dir + 'Configuracion.xlsx'
			set @cmd = 'dir "' + @path + '" /b'

			set @srv = etl.fn_getParametroGral('EXCEL-SRV')
			set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path

			-- Salarios Minimos
			set @sql = 'insert into #SalariosMinimos (num_cia, compañia, salario_minimo)
				select num_cia, 
					compañia, 
					cast(replace(salario_minimo, '','', ''.'') as float) as salario_minimo
				FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Salario-Min$]'') X
				WHERE 1=1
					and num_cia is not null'
		
			exec (@sql)
			set @msg = 'Ins. Salarios-Minimo: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

			-- Conceptos Percepcion
			set @sql = 'insert into #ConceptosPercepcion (Concepto, Descripcion, Suma_Dias)
				select etl.fn_trim(Concepto) as Concepto, 
					etl.fn_trim(Descripcion) as Descripcion, 
					isnull(etl.fn_trim(Suma_Dias),''No'') as Suma_Dias
				FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Concep-Percep$]'') X
				WHERE 1=1
					and Concepto is not null'
		
			exec (@sql)
			set @msg = 'Ins. Concep. Persepcion: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

			-- Deducciones Prioridad
			set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=No;IMEX=1;FMT=Fixed;Database='+@path
			set @sql = 'insert into #DeduccionesPrioridad (
					Agrupacion_Paises, CC_Nomina, Fin_Validez, Inicio_Validez, Nro_Orden,
					Concepto_Human, Texto_CC_Nomina, Prioridad_Deduc_Moras, Caracteristica_Demora,
					Desc_Aplicacion_Descuento)
				select Agrupacion_Paises, CC_Nomina, Fin_Validez, Inicio_Validez, Nro_Orden,
					Concepto_Human, Texto_CC_Nomina, Prioridad_Deduc_Moras, Caracteristica_Demora,
					Desc_Aplicacion_Descuento
				from (
					select f1 as Agrupacion_Paises,
						f2 as CC_Nomina,
						f3 as Fin_Validez,
						f4 as Inicio_Validez,
						cast((case when iSnumeric(f5) = 1 then isnull(f5,0) else 0 end) as numeric(10)) as Nro_Orden,
						f6 as Concepto_Human,
						f7 as Texto_CC_Nomina,
						f8 as Prioridad_Deduc_Moras,
						f9 as Caracteristica_Demora,
						f10 as Desc_Aplicacion_Descuento
				FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Deduc-Prioridad$]'')) X
				WHERE 1=1
					and Nro_Orden > 0'
		
			exec (@sql)
			set @msg = 'Ins. Deducciones Prioridad: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg
			
		end try
		begin catch
			set @msg = 'Error al Cargar Archivo Configuracion - Msg: ' + isnull(ERROR_MESSAGE(),'<null>')
			exec etl.prc_Logging @idBatch, @msg
			return -1
		end catch

		-- select * from #SalariosMinimos
		-- select * from #ConceptosPercepcion
		-- select * from #DeduccionesPrioridad
		
		-- Cargar Archivo de Empleados
		set @msg = '- Load Archivos Empleados: '; exec etl.prc_Logging @idBatch, @msg
		
		-- Obtener Archivos a Procesar (MC y MF)
		set @path = @dir + 'M*.xls*'
		set @cmd = 'dir "' + @path + '" /b'
	
		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		delete from #tmpArchivo
		where archivo is null
			or left(archivo,2) not in ('MF', 'MC')

		select archivo from #tmpArchivo
		return

		-- Validar Si hay Archivos
		if not exists( select 1 from #tmpArchivo where archivo like '%.xls%')
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
				@dir + archivo as path
			from #tmpArchivo
			order by 1
		
			open lc_cursor
			fetch next from lc_cursor into @archivo, @path
			while @@fetch_status = 0
				begin
					set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path

					set @sql = '
						insert into #ConceptoEmpleados (sociedad, cia_adecco, nombre_cia, proceso, descr_proceso, tipo_empleado, anio_proceso, 
							num_per_proceso, fecha_inicio_proceso, fecha_fin_proceso, mes_contable_proceso, num_sem_proceso, 
							perner, numero_empleado, paterno_empleado, materno_empleado, nombre_empleado, nss, area_personal, area_personal1, 
							registro_patronal, fecha_ingreso, fecha_antiguedad, division, descr_division, 
							subdivision, descr_subdivision, descr_centro_costo, centro_costo, concepto, 
							descr_concepto, variable_tiempo, importe) 
						SELECT sociedad, cia_adecco, nombre_cia, proceso, descr_proceso, tipo_empleado, anio_proceso, 
							num_per_proceso, fecha_inicio_proceso, fecha_fin_proceso, mes_contable_proceso, num_sem_proceso, 
							perner, numero_empleado, paterno_empleado, materno_empleado, nombre_empleado, nss, area_personal, area_personal1, 
							registro_patronal, fecha_ingreso, fecha_antiguedad, division, descr_division, 
							subdivision, descr_subdivision, descr_centro_costo, centro_costo, concepto, 
							descr_concepto, variable_tiempo, importe
						FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Info$]'') X
						WHERE 1=1 and cia_adecco is not null'

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
					fetch next from lc_cursor into @archivo, @path
				end
			close lc_cursor
			deallocate lc_cursor

			set @msg = '- Carga Stage: '; exec etl.prc_Logging @idBatch, @msg
			
			truncate table stg.ST_ODS_DN_CONFIG_SALARIO 
			truncate table stg.ST_ODS_DN_CONFIG_PERCEPCION
			truncate table stg.ST_ODS_DN_CONFIG_PRIORIDAD
			truncate table stg.ST_ODS_DN_NOMINA_CONCEPTO

			insert into stg.ST_ODS_DN_CONFIG_SALARIO (
				periodo, num_cia, compañia, salario_minimo)
			select @periodo, 
				num_cia, compañia, salario_minimo
			from #SalariosMinimos

			set @msg = 'Ins. Salarios  Minimos: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

			insert into stg.ST_ODS_DN_CONFIG_PERCEPCION(
				periodo, concepto, descripcion, suma_dias)
			select @periodo, 
				concepto, descripcion, suma_dias
			from #ConceptosPercepcion

			set @msg = 'Ins. Percepcion Conceptos: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

			insert into stg.ST_ODS_DN_CONFIG_PRIORIDAD (
				periodo, nro_orden, concepto_human, texto_cc_nomina,
				prioridad_deduc_moras,
				caracteristica_demora,
				desc_aplicacion_descuento )
			select @periodo,
				nro_orden, concepto_human, texto_cc_nomina,
				prioridad_deduc_moras,
				caracteristica_demora,
				desc_aplicacion_descuento
			from #DeduccionesPrioridad
			set @msg = 'Ins. Deducciones: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

			insert into stg.ST_ODS_DN_NOMINA_CONCEPTO (
				periodo, num_cia, proceso, anio_proceso, num_per_proceso, mes_contable_proceso,
				num_sem_proceso, tipo_empleado, numero_empleado, 
				paterno_empleado, materno_empleado, nombre_empleado,
				concepto, descr_concepto,
				dias, importe)
			select @periodo,
				cia_adecco, proceso, anio_proceso, num_per_proceso, mes_contable_proceso,
				num_sem_proceso, tipo_empleado, numero_empleado, 
				paterno_empleado, materno_empleado, nombre_empleado,
				concepto, descr_concepto,
				isnull(variable_tiempo, 0) as dias,
				isnull(importe,0) as importe
			from #ConceptoEmpleados
			where cia_adecco is not null
			
			set @cant = @@rowCount
			set @msg = 'Ins. Nomina Concepto: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg
			
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
-- exec etl.load_ods_deduccion_nomina 0