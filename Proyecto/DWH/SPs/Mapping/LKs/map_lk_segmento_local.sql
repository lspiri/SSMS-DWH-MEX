use MX_DWH
Go

if object_id ('etl.map_lk_segmento_local', 'P') is not null
	drop procedure etl.map_lk_segmento_local
GO

create procedure etl.map_lk_segmento_local (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21/03/2022
	-- Description:	Mapping LK Segmento Local
	-- =============================================
	-- 21/03/2022 - Version Inicial

	set nocount on;
	begin try

		declare @msg varchar(500)
		
		set @msg = '>> Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		-- Bajada
		select etl.fn_trim(cod_segmento_local) as codigo, 
			etl.fn_trim(desc_segmento_local) as descripcion
		into #datos
		from (
			select -1 as orden,
				'-1' as cod_segmento_local, 
				'Not Exists' as desc_segmento_local
			union all
			select 0 as orden,
				'NA' as cod_segmento_local, 
				'Not Available' as desc_segmento_local
			union all
			select 1 as orden, cod_segmento_local, max(isnull(desc_segmento_local,''))
			from stg.ST_DCTE_LK_SEGMENTO_LOCAL
			where cod_segmento_local is not null and cod_segmento_local <> 'NA'
			group by cod_segmento_local) x
		order by x.orden, x.cod_segmento_local

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		-- Insertar Nuevo
		insert into dbo.DCTE_LK_SEGMENTO_LOCAL (
			cod_segmento_local, desc_segmento_local)
		select codigo, descripcion
		from #datos a
		where not exists (select 1 
			from dbo.DCTE_LK_SEGMENTO_LOCAL b
			where b.cod_segmento_local = a.codigo)
		
		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK
		set desc_segmento_local = b.descripcion
		from #datos b, dbo.DCTE_LK_SEGMENTO_LOCAL LK
		where b.codigo = LK.cod_segmento_local
		 and (b.descripcion <> LK.desc_segmento_local)
		
		set @msg = 'Actualizados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		commit transaction

	end try

	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch
	end catch
end
GO
-- exec etl.map_lk_segmento_local 0