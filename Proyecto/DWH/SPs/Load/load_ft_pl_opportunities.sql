use MX_DWH
Go

if object_id ('etl.load_ft_pl_opportunities', 'P') is not null
	drop procedure etl.load_ft_pl_opportunities
GO

create procedure etl.load_ft_pl_opportunities (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	06/06/2022
	-- Description:	Carga Archivo Budget Rate para Opps
	-- =============================================
	-- 06/06/2022 - Version Inicial
	
	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10),
			@dir varchar(255), @path varchar(255), @Cmd varchar(255),
			@archivo varchar(150), @src varchar(255), @sql varchar(2000),
			@srv varchar(255), @Año varchar(30)

		-- drop table #tmpArchivo
		create table #tmpArchivo(id int identity, archivo varchar(255))
		create table #info (
			año int,
			pais varchar(30) COLLATE database_default, 
			cod_moneda varchar(30) COLLATE database_default, 
			eur_x_loc float,	
			loc_x_eur float
		) 

		-- Inicio
		set @msg = '>>- Stage: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Obtener Archivos a Procesar
		set @dir = etl.fn_getDirectorioLoad(@idBatch) + 'PL_Oportunidades\'
		set @path = @dir + 'Budget Rate*.xlsx'
		set @cmd = 'dir "' + @path + '" /b'

		set @Año = cast(year(getdate()) as varchar)
		set @msg = 'Anio: ' + cast(@Año as varchar); exec etl.prc_Logging @idBatch, @msg

		insert into #tmpArchivo
		exec master..xp_cmdshell @cmd

		delete from #tmpArchivo
		where archivo is null
			or archivo not like  '%' + @Año + '%'
			or archivo not like '%.xlsx'

		-- Validar Si hay Archivos
		if not exists( select 1 from #tmpArchivo)
			begin
				set @msg = 'No existen archivo para procesar, el proceso no continua.'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				return 0
			end
		
		select top 1 @archivo = archivo 
		from #tmpArchivo

		-------------------------------------------
		-- PROCESAR ARCHIVOS 
		-------------------------------------------
		set @archivo = @dir + @archivo 
		set @msg = 'Cargar de Archivo: ' + @archivo; exec etl.prc_Logging @idBatch, @msg
		set @srv = etl.fn_getParametroGral('EXCEL-SRV')
		set @src = etl.fn_getParametroGral('EXCEL-SRC') + ';HDR=yes;IMEX=1;FMT=Fixed;Database='+@archivo

		set @sql = 'insert into #info (año, pais, cod_moneda, eur_x_loc, loc_x_eur) 
			select '+@Año+' as año, pais, moneda,
				[EUR X LOC],
				[LOC X EUR]
			FROM OPENROWSET('''+@srv+''', '''+@src+''', ''SELECT * FROM [info$]'') X
			WHERE 1=1
				and [Pais] is not null'

		begin try
			exec (@sql)
			set @cant = @@rowCount
		end try

		begin catch
			set @cant = 0
			set @msg = '<b>Warning: : ' + isnull(ERROR_MESSAGE(),'<null>') + '</b>'
			exec etl.prc_Logging @idBatch, @msg
		end catch

		set @msg = 'Temporal: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		begin transaction

			
			exec etl.prc_Logging @idBatch, '--'

			if @cant > 0
				begin

					delete from dbo.ODS_PL_OPP_BUDGET_RATE
					where Año = @Año

					set @msg = 'Borrados: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

					insert into dbo.ODS_PL_OPP_BUDGET_RATE (
						Año, cod_pais, cod_moneda, Eur_x_Loc, Loc_x_Eur)
					select Año, Case pais 
						when 'Argentina' then 'AR'
						when 'Brazil' then 'BR'
						when 'Chile' then 'CL'
						when 'Colombia' then 'CO'
						when 'Ecuador' then 'EC'
						when 'Mexico' then 'MX'
						when 'Peru' then 'PE'
						when 'Uruguay' then 'UY'
						else left(pais,2) end cod_pais,
						cod_moneda,
						isnull(Eur_x_Loc,0), 
						isnull(Loc_x_Eur,0)
					from #info 

					set @msg = 'Insertados: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg
				end
			
			

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
-- exec etl.load_ft_pl_opportunities 0