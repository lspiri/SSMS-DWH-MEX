use MX_DWH
Go

if object_id ('etl.load_ods_cnrp_excepcion', 'P') is not null
	drop procedure etl.load_ods_cnrp_excepcion
GO

create procedure etl.load_ods_cnrp_excepcion (
	@idBatch numeric(15),
	@idMes numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	17/05/2022
	-- Description:	Load Excepciones CNRP
	-- =============================================
	-- 02/06/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(150), @src varchar(255), @sql varchar(2000),
			@srv varchar(255)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))
		create table #excepcion (
			AÑO	varchar(30) COLLATE database_default,
			COD_CLIENTE varchar(30) COLLATE database_default,
			COD_SUCURSAL varchar(30) COLLATE database_default,
			COD_SERVICIO varchar(30) COLLATE database_default,
			COD_EMPRESA varchar(30) COLLATE database_default,
			ESTADO	varchar(30) COLLATE database_default,
			CLIENTE varchar(255) COLLATE database_default)

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @msg = 'Mes: ' + cast(@idMes as varchar); exec etl.prc_Logging @idBatch, @msg

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'CNRP\'
		set @path = @dir + 'Excepciones.xlsx'
		set @cmd = 'dir "' + @path + '" /b'

		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		-- Lineas_EstadoGestion.xls
		delete from #tmpArchivo
		where archivo is null or archivo not like '%.xlsx'

		-- Validar Si hay Archivos
		if not exists( select 1 from #tmpArchivo)
			begin
				set @msg = 'No existe archivo para procesar, el proceso no continua.'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				return 0
			end
		
		-------------------------------------------
		-- PROCESAR ARCHIVOS 
		-------------------------------------------
		set @msg = 'Cargar Archivo: ' + @path; exec etl.prc_Logging @idBatch, @msg
		set @srv = etl.fn_getParametroGral('EXCEL-SRV')
		set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=yes;IMEX=1;FMT=Fixed;Database='+@path

		set @sql = '
			insert into #excepcion (año, cod_cliente, cod_sucursal, cod_servicio, cod_empresa, estado, cliente) 
			SELECT  etl.fn_trim(año) as año,
				etl.fn_trim(cod_cliente) as cod_cliente,
				etl.fn_trim(cod_sucursal) as cod_sucursal,
				etl.fn_trim(cod_servicio) as cod_servicio,
				etl.fn_trim(cod_empresa) as cod_empresa,
				etl.fn_trim(estado) as estado,
				etl.fn_trim(cliente) as cliente
			FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Info$]'') X
			WHERE 1=1
				and año is not null'


		exec (@sql)

		set @cant = @@rowCount 
		set @msg = 'Insertados: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		begin transaction

			truncate table stg.ST_ODS_CNRP_EXCEPCION

			insert into stg.ST_ODS_CNRP_EXCEPCION (año, cod_cliente, cod_sucursal, cod_servicio, cod_empresa, estado, cliente) 
			select distinct año, cod_cliente, cod_sucursal, cod_servicio, cod_empresa, estado, cliente
			from #excepcion
			where año = left(cast(@idMes as varchar),4)

			set @msg = 'Insertados Stage: ' + cast(@@rowCount as varchar); exec etl.prc_Logging @idBatch, @msg

		commit transaction

		-- Ok
		return 1

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
/*
exec etl.load_ods_cnrp_excepcion 0, 202109
exec etl.load_ods_cnrp_excepcion 0, 202110
exec etl.load_ods_cnrp_excepcion 0, 202111
exec etl.load_ods_cnrp_excepcion 0, 202112

exec etl.load_ods_cnrp_excepcion 0, 202201
exec etl.load_ods_cnrp_excepcion 0, 202202
exec etl.load_ods_cnrp_excepcion 0, 202203
*/