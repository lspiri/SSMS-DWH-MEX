use MX_DWH
Go

if object_id ('etl.map_lk_dop', 'P') is not null
	drop procedure etl.map_lk_dop
GO

create procedure etl.map_lk_dop (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21/03/2022
	-- Description:	Mapping LK DOP
	-- =============================================
	-- 21/03/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @cant numeric(10), @novedad numeric(10)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @novedad = 0

		begin transaction

		-- Bajada
		select etl.fn_trim(codigo) as codigo, 
			etl.fn_trim(descripcion) as descripcion,
			etl.fn_trim(pais) as pais
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion,
				'Not Exists' as pais
			union all
			select 0 as orden,
				'NA' as codigo, 
				'Not Available' as descripcion,
				'Not Available' as pais
			union all
			select 2 as orden, cod_dop, 
				max(isnull(desc_dop,'')),
				max(isnull(pais,'Argentina'))
			from stg.ST_DGEO_LK_DOP
			where cod_dop is not null
			group by cod_dop) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		-- Insertar Nuevo
		insert into dbo.DGEO_LK_DOP (
			cod_dop, desc_dop, pais)
		select codigo, descripcion, pais
		from #datos a
		where not exists (select 1 
			from dbo.DGEO_LK_DOP b
			where b.cod_dop = a.codigo)

		set @cant = @@rowcount
		set @novedad = @novedad + @cant

		set @msg = 'Insertados: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update dbo.DGEO_LK_DOP
		set desc_dop = b.descripcion,
			pais = b.pais
		from #datos b
		where b.codigo = dbo.DGEO_LK_DOP.cod_dop
			and (b.descripcion <> dbo.DGEO_LK_DOP.desc_dop
			or b.pais <> dbo.DGEO_LK_DOP.pais)

		set @cant = @@rowcount
		set @novedad = @novedad + @cant

		set @msg = 'Actualizados: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg

		commit transaction

		if (@novedad) = 0 
			return 0
		else
			return 1

	end try

	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch

		return -1

	end catch
end
GO
-- exec etl.map_lk_dop 0