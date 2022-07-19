use MX_DWH
Go

if object_id ('etl.load_lk_linea_esg', 'P') is not null
	drop procedure etl.load_lk_linea_esg
GO

create procedure etl.load_lk_linea_esg (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	31/03/2022
	-- Description:	Carga Archivo Lineas Estado de Gestion
	-- =============================================
	-- 16/06/2022 - Version Inicial
	
	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(150), @src varchar(255), @sql varchar(2000),
			@srv varchar(255)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))
		create table #temp_lines (
			COD_LINEA varchar(30) COLLATE database_default, 
			COD_CUENTA varchar(30) COLLATE database_default, 
			COD_SERVICIO varchar(30) COLLATE database_default, 
			DESC_LINEA varchar(255) COLLATE database_default, 
			DESC_LINEA_N2 varchar(255) COLLATE database_default,
			ID_LINEA1 numeric(15),
			ID_LINEA2 numeric(15),
			CALCULO varchar(500) COLLATE database_default) 

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'OPR\'
		set @path = @dir + 'Lineas_EstadoGestion.xlsx'
		set @cmd = 'dir "' + @path + '" /b'

		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		-- Lineas_EstadoGestion.xls
		delete from #tmpArchivo
		where archivo is null

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
		set @msg = 'Cargar Archivo: ' + @path; exec etl.prc_Logging @idBatch, @msg
		set @srv = etl.fn_getParametroGral('EXCEL-SRV')
		set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=yes;IMEX=1;FMT=Fixed;Database='+@path

		set @sql = '
			insert into #temp_lines(COD_LINEA, COD_CUENTA, COD_SERVICIO,
				DESC_LINEA, DESC_LINEA_N2, ID_LINEA1, ID_LINEA2,
				CALCULO) 
			SELECT  etl.fn_trim(Codigo) as COD_LINEA,
				etl.fn_trim(Cuenta) as COD_CUENTA,
				etl.fn_trim(LineaServicio) as COD_SERVICIO,
				etl.fn_trim(Nivel1) as DESC_LINEA,
				etl.fn_trim(Nivel2) as DESC_LINEA_N2 ,		
				IdNivel1 as ID_LINEA1,
				IdNivel2 as ID_LINEA2,
				Calculo
			FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Lineas$]'') X
			WHERE 1=1
				and codigo is not null'


		exec (@sql)

		set @cant = @@rowCount 
		set @msg = 'Insertados: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		begin transaction

			truncate table stg.ST_DESG_LK_LINEA

			insert into stg.ST_DESG_LK_LINEA (cod_linea, cod_cuenta, cod_servicio,
				desc_linea, desc_linea_n2, desc_linea_n3,
				id_linea2, id_linea3, calculo) 
			select cod_linea, cod_cuenta, cod_servicio,
				desc_linea, desc_linea_n2, null desc_linea_n3,
				id_linea2, null as id_linea3, 
				calculo
			from #temp_lines 

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
-- exec etl.load_lk_linea_esg 0