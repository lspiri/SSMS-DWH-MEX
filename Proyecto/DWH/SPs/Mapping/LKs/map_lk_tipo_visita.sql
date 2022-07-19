use MX_DWH
Go

if object_id ('etl.map_lk_tipo_visita', 'P') is not null
	drop procedure etl.map_lk_tipo_visita
GO

create procedure etl.map_lk_tipo_visita (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 22/04/2022
	-- Description:	Mapping LK Tipo Visita
	-- =============================================
	-- 22/04/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500)

		-- Inicio
		set @msg = '>> Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		-- Viene Sin Codigo de SF
		update st
		set cod_tipo_visita = 'SF-' + cast(etl.fn_getValorAscii (desc_tipo_visita) as varchar)
		from stg.ST_DVIS_LK_TIPO_VISITA st
		where cod_tipo_visita = '<SF>'
		
		-- Bajada
		select codigo as codigo, 
			etl.fn_trim(descripcion) as descripcion
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion
			union all
			select 1 as orden,
				'NA' as codigo, 
				'Not Available' as descripcion
			union all
			select 2 as orden, cod_tipo_visita, 
				max(isnull(desc_tipo_visita,''))
			from stg.ST_DVIS_LK_TIPO_VISITA
			where cod_tipo_visita is not null
			group by cod_tipo_visita) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		-- Update Codigo
		update lk
		set cod_tipo_visita = a.codigo
		from #datos a, dbo.DVIS_LK_TIPO_VISITA lk
		where 1=1
			and a.descripcion = lk.desc_tipo_visita
			and a.codigo != lk.cod_tipo_visita
			and a.codigo like 'SF-%'

		set @msg = 'Actualiza Codigo: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Insertar Nuevo
		insert into dbo.DVIS_LK_TIPO_VISITA (
			cod_tipo_visita, desc_tipo_visita)
		select codigo, descripcion
		from #datos a
		where not exists (select 1 
			from dbo.DVIS_LK_TIPO_VISITA b
			where b.cod_tipo_visita = a.codigo)

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update dbo.DVIS_LK_TIPO_VISITA
		set desc_tipo_visita = b.descripcion
		from #datos b
		where b.codigo = dbo.DVIS_LK_TIPO_VISITA.cod_tipo_visita
			and (b.descripcion <> dbo.DVIS_LK_TIPO_VISITA.desc_tipo_visita)

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
-- exec etl.map_lk_tipo_visita 0