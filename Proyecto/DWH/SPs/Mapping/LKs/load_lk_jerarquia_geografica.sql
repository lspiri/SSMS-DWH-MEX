use MX_DWH
Go

if object_id ('etl.load_lk_jerarquia_geografica', 'P') is not null
	drop procedure etl.load_lk_jerarquia_geografica
GO

create procedure etl.load_lk_jerarquia_geografica (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	21/03/2022
	-- Description:	Carga Archivos Jerarquia Geografica
	-- =============================================
	-- 21/03/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@src varchar(255), @sql varchar(2000), @srv varchar(255),
			@periodo varchar(6), @proceso varchar(30)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @periodo = year(getdate()) * 100 +  month(getdate()) 
		
		if @idBatch > 0
			set @proceso = etl.fn_getProcesoBatch(@idBatch)
		else 
			set @proceso = 'LK_JERARQUIA_GEOGRAFICA'

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'Jerarquia_GEO\'
		set @msg = 'Dir: ' + @dir; exec etl.prc_Logging @idBatch, @msg

		set @path = @dir + '*.xlsx'
		set @cmd = 'dir "' + @path + '" /b'
		set @srv = etl.fn_getParametroGral('EXCEL-SRV')
		
		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		delete from #tmpArchivo
		where archivo is null
			or archivo not like '%xlsx%'

		-- Validar Si hay Archivos
		if not exists( select 1 from #tmpArchivo)
			begin
				set @msg = 'No existen archivos para procesar, el proceso no continua.'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				exec etl.prc_reg_ctrl_mapping @msg, @idBatch
				return 0
			end

		-------------------------------------------
		-- PROCESAR ARCHIVOS 
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'Archivos Procesados:'

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

		-- Truncar
		truncate table stg.ST_DGEO_LK_DOP
		truncate table stg.ST_DGEO_LK_REGION
		truncate table stg.ST_DGEO_LK_SUCURSAL
		truncate table stg.ST_DGEO_LK_ZONA

		---------------------
		-- DOP
		---------------------
		set @path = @dir + 'DOP.xlsx'
		set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path
		
		set @sql = 'insert into stg.ST_DGEO_LK_DOP (cod_dop, desc_dop, pais)
					SELECT cod_dop, nombre_dop, pais
					FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [info$]'') x 
					WHERE 1=1
						and cod_dop is not null'
		exec (@sql)

		set @cant = @@rowCount
		set @msg = 'Load Archivo DOP - Ins: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		if @cant > 0 
		begin
			insert into stg.ST_CONTROL_ARCHIVOS (
				periodo, proceso, archivo, path, registros, procesado)
			values ( @periodo, @proceso, @path, @dir, @cant, 'N')
		end

		---------------------
		-- REGION
		---------------------
		set @path = @dir + 'REGION.xlsx'
		set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path
		
		set @sql = 'insert into stg.ST_DGEO_LK_REGION (cod_region, desc_region, cod_dop, responsable, dr_comite, fte)
				SELECT cod_region, desc_region, cod_dop, responsable, dr_comite, fte
				FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [info$]'') x 
				WHERE 1=1
					and cod_region is not null'
		exec (@sql)

		set @cant = @@rowCount
		set @msg = 'Load Archivo REGION - Ins: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		if @cant > 0 
		begin
			insert into stg.ST_CONTROL_ARCHIVOS (
				periodo, proceso, archivo, path, registros, procesado)
			values ( @periodo, @proceso, @path, @dir, @cant, 'N')
		end
		
		---------------------
		-- ZONA
		---------------------
		set @path = @dir + 'ZONA.xlsx'
		set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path
		
		set @sql = 'insert into stg.ST_DGEO_LK_ZONA (cod_zona, desc_zona)
					SELECT cod_zona, nombre_zona
					FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [info$]'') x 
					WHERE 1=1
						and cod_zona is not null'
		exec (@sql)

		set @cant = @@rowCount
		set @msg = 'Load Archivo ZONA - Ins: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		if @cant > 0 
		begin
			insert into stg.ST_CONTROL_ARCHIVOS (
				periodo, proceso, archivo, path, registros, procesado)
			values ( @periodo, @proceso, @path, @dir, @cant, 'N')
		end
		
		---------------------
		-- SUCURSAL
		---------------------
		set @path = @dir + 'SUCURSAL.xlsx'
		set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=Yes;IMEX=1;FMT=Fixed;Database='+@path
		
		set @sql = 'insert into stg.ST_DGEO_LK_SUCURSAL (cod_sucursal, desc_sucursal, marca_ms, 
				cod_region, agrupador, cod_zona)
			SELECT cod_sucursal, desc_sucursal, ''N'' marca_ms, cod_region, 
				agrupador, cod_zona
			FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [info$]'') x 
			WHERE 1=1
				and cod_sucursal is not null'
		exec (@sql)

		set @cant = @@rowCount
		set @msg = 'Load Archivo SUCURSAL - Ins: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		if @cant > 0 
		begin
			insert into stg.ST_CONTROL_ARCHIVOS (
				periodo, proceso, archivo, path, registros, procesado)
			values ( @periodo, @proceso, @path, @dir, @cant, 'N')
		end

		commit transaction

		-- Ok
		return 1

	end try

	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'LOAD', @idBatch

		-- Error
		return -1

	end catch
end
GO
-- exec etl.load_lk_jerarquia_geografica 0