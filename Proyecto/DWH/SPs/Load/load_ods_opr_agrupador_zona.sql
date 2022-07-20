use MX_DWH
Go

if object_id ('etl.load_ods_opr_agrupador_zona', 'P') is not null
	drop procedure etl.load_ods_opr_agrupador_zona
GO

create procedure etl.load_ods_opr_agrupador_zona (
	@idBatch numeric(15), @idMes numeric(10))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	26/05/2022
	-- Description:	Carga Archivo CC OPR Agrupadores
	-- =============================================
	-- 02/06/2022 - Version Inicial
	-- 20/07/2022 - LS - Orden Present Validacion
	
	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(150), @src varchar(255), @sql varchar(2000),
			@srv varchar(255), @idMesCarga numeric(10), @DescMes varchar(30)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))
		create table #cc_opr (
			id_mes int,
			orden_present int,
			cod_cc varchar(30) COLLATE database_default, 
			cod_agrupador varchar(30) COLLATE database_default, 
			descripcion varchar(255) COLLATE database_default, 
			desc_zona varchar(255) COLLATE database_default, 
			dir_prorrateo varchar(255) COLLATE database_default,
			Allocation varchar(255) COLLATE database_default
		) 

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'OPR\'
		set @path = @dir + 'Agrupadores_Zona*.xlsx'
		set @cmd = 'dir "' + @path + '" /b'

		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		delete from #tmpArchivo
		where archivo is null
			or archivo not like  '%' + left(cast(@idMes as varchar),4) + '%'
			or archivo not like '%.xlsx'

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
		set @msg = 'Cargar de Archivos: ' + @path; exec etl.prc_Logging @idBatch, @msg
		set @srv = etl.fn_getParametroGral('EXCEL-SRV')

		declare lc_cursor cursor for
		select c.archivo,
			@dir + c.archivo as path,
			a.id_mes, a.desc_nro_mes as mes
		from dbo.DTPO_LK_MESES a,
			dbo.DTPO_LK_MESES b,
			#tmpArchivo c
		where a.id_mes <= b.id_mes
			and a.anio = b.anio
			and b.id_mes = @idMes
		order by a.id_mes
		
		open lc_cursor
		fetch next from lc_cursor into @archivo, @path, @idMesCarga, @DescMes
		while @@fetch_status = 0
			begin

				set @msg = 'Mes: ' + @DescMes; exec etl.prc_Logging @idBatch, @msg
				set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=yes;IMEX=1;FMT=Fixed;Database='+@path

				set @sql = '
					insert into #cc_opr (id_mes, orden_present, cod_cc, cod_agrupador,
						descripcion, desc_zona, dir_prorrateo, Allocation) 
					select '+cast(@idMesCarga as varchar)+' as id_mes,
						case when isNumeric(present) = 0 or present is null then 0 else present end as orden_present,
						cast(replace([CC ORIGEN],''MX_'','''') as varchar(30)) as cod_cc,
						cast(replace(AGRUPADOR,''MX_'','''') as varchar(30)) as cod_agrupador,	
						cast(DESCRIPCION as varchar(255)) as descripcion,
						cast(ZONA as varchar(255)) as desc_zona,
						cast([DIR PRORRATEO] as varchar(30)) as dir_prorrateo,
						cast([Allocation] as varchar(30)) as Allocation
					FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM ['+ @DescMes +'$]'') X
					WHERE 1=1
						and [CC ORIGEN] is not null'

				begin try
					exec (@sql)
					set @cant = @@rowCount
				end try

				begin catch
					set @cant = 0
					set @msg = '<b>Warning: : ' + isnull(ERROR_MESSAGE(),'<null>') + '</b>'
					exec etl.prc_Logging @idBatch, @msg
				end catch

				set @msg = 'Insertados: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

				-- next
				fetch next from lc_cursor into @archivo, @path, @idMesCarga, @DescMes
			end
		close lc_cursor
		deallocate lc_cursor

		
		begin transaction
			exec etl.prc_Logging @idBatch, '--'

			truncate table stg.ST_ODS_OPR_AGRUPADOR_ZONA

			insert into stg.ST_ODS_OPR_AGRUPADOR_ZONA (periodo, orden_present, 
				cod_cc, cod_agrupador, descripcion, cod_zona, desc_zona, 
				dir_prorrateo, allocation) 
			select id_mes, orden_present, 
				etl.fn_trim(cod_cc) as cod_cc, 
				etl.fn_trim(cod_agrupador) as cod_agrupador, 
				etl.fn_trim(descripcion) as descripcion,
				z.cod_zona,
				isnull(z.desc_zona,vw.desc_zona) as desc_zona,	
				etl.fn_trim(dir_prorrateo),	
				etl.fn_trim(Allocation)	
			from (select a.*, 
				etl.fn_getCodListaDGCDesc( a.desc_zona ) as cod_zona
			from (select distinct * from #cc_opr) a) vw
				left outer join dbo.DGEO_LK_ZONA z
					on z.cod_zona = vw.cod_zona
			order by id_mes, etl.fn_trim(cod_agrupador), 
				case when orden_present = 0 then 1000 else orden_present end,
				etl.fn_trim(cod_cc)

			set @cant = @@rowCount
			set @msg = 'Insertados Stage: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

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
-- exec etl.load_ods_opr_agrupador_zona 0, 202206