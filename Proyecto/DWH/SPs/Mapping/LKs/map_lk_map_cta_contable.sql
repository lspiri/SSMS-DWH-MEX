use MX_DWH
Go

if object_id ('etl.map_lk_map_cta_contable', 'P') is not null
	drop procedure etl.map_lk_map_cta_contable
GO

create procedure etl.map_lk_map_cta_contable (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 28/03/2022
	-- Description:	Mapping LK Pri Servicio
	-- =============================================
	-- 28/03/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500)

		-- Inicio
		set @msg = '>> Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

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
			select distinct 2 as orden,
				st.cod_cuenta, 
				st.desc_cuenta
			from stg.ST_DGEN_LK_CTA_CONTABLE st
		) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		begin transaction

		-- Insertar Nuevo
		insert into dbo.DGEN_LK_CTA_CONTABLE (
			cod_cuenta, desc_cuenta)
		select codigo, descripcion
		from #datos a
		where not exists (select 1 
			from dbo.DGEN_LK_CTA_CONTABLE b
			where b.cod_cuenta = a.codigo)

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK  
		set desc_cuenta = b.descripcion
		from dbo.DGEN_LK_CTA_CONTABLE LK, #datos b
		where b.codigo = LK.cod_cuenta
			and (b.descripcion <> LK.desc_cuenta)

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
-- exec etl.map_lk_map_cta_contable 0