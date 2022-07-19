use MX_DWH
Go

if object_id ('etl.map_lk_zona', 'P') is not null
	drop procedure etl.map_lk_zona
GO

create procedure etl.map_lk_zona (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21/03/2022
	-- Description:	Mapping LK Zona
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
			etl.fn_trim(descripcion) as descripcion
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as descripcion
			union all
			select 0 as orden,
				'NA' as codigo, 
				'Not Available' as descripcion
			union all
			select 2 as orden, cod_zona, 
				max(isnull(desc_zona,''))
			from stg.ST_DGEO_LK_ZONA
			where cod_zona is not null
			group by cod_zona) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		-- Insertar Nuevo
		insert into dbo.DGEO_LK_ZONA (
			cod_zona, desc_zona)
		select codigo, descripcion
		from #datos a
		where not exists (select 1 
			from dbo.DGEO_LK_ZONA b
			where b.cod_zona = a.codigo)

		set @cant = @@rowcount
		set @novedad = @novedad + @cant

		set @msg = 'Insertados: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update dbo.DGEO_LK_ZONA
		set desc_zona = b.descripcion
		from #datos b
		where b.codigo = dbo.DGEO_LK_ZONA.cod_zona
			and b.descripcion <> dbo.DGEO_LK_ZONA.desc_zona

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
-- exec etl.map_lk_zona 0