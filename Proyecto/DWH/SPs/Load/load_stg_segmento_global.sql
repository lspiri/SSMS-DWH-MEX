use MX_DWH
Go

if object_id ('etl.load_stg_segmento_global', 'P') is not null
	drop procedure etl.load_stg_segmento_global
GO

create procedure etl.load_stg_segmento_global (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	17/03/2022
	-- Description:	Carga Archivo Segmento Global
	-- =============================================
	-- 18/07/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(150), @src varchar(255), @sql varchar(2000),
			@proceso varchar(30), @srv varchar(255), @periodo varchar(6)


		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @periodo = year(getdate()) * 100 +  month(getdate()) 
		
		if @idBatch > 0
			set @proceso = etl.fn_getProcesoBatch(@idBatch)
		else 
			set @proceso = 'DCTE_LK_CLIENTE'

		set @msg = 'Proceso: ' + @proceso; exec etl.prc_Logging @idBatch, @msg				
		set @msg = 'Periodo: ' + @periodo; exec etl.prc_Logging @idBatch, @msg

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'Info_Cliente\'
		set @path = @dir + 'Segmentacion_Global.xlsx'
		set @cmd = 'dir "' + @path + '" /b'

		set @msg = 'Directorio: ' + @dir; exec etl.prc_Logging @idBatch, @msg

		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		delete from #tmpArchivo
		where archivo is null

		-- Validar Si hay Archivos
		if not exists( select 1 from #tmpArchivo)
			begin
				set @msg = 'No existen archivos para procesar, el proceso no continua.'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				return 0
			end

		set @srv = etl.fn_getParametroGral('EXCEL-SRV')

		-------------------------------------------
		-- PROCESAR ARCHIVOS 
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'Archivos Procesados:'
		
		begin transaction

		-- Truncar
		truncate table stg.ST_DCTE_SEGMENTO_GLOBAL

		/*
		delete stg.ST_CONTROL_ARCHIVOS 
		where proceso = @proceso
			and periodo = @periodo
			and procesado <> 'S'

		update stg.ST_CONTROL_ARCHIVOS 
		set procesado = 'X'
		where proceso = @proceso
			and procesado = 'N'
		*/

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

				
				set @sql = 'insert into stg.ST_DCTE_SEGMENTO_GLOBAL (archivo, periodo, 
						cuit, cliente, segmento)
					select '''+@archivo+''', '''+@periodo+''', 
						CUIT, Cliente, Nueva_Segmentacion_Global
						FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [Info$]'') X
					where 1=1
						and cuit is not null'

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

				/*
				if @cant > 0 
					begin
						insert into stg.ST_CONTROL_ARCHIVOS (
							periodo, proceso, archivo, path, registros, procesado)
						values ( @periodo, @proceso, @path, @dir, @cant, 'N')
					end
				*/

				-- next
				fetch next from lc_cursor into @archivo, @path
			end
		close lc_cursor
		deallocate lc_cursor
				
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
-- exec etl.load_stg_segmento_global 0