use MX_DWH
Go

if object_id ('etl.map_lk_contacto', 'P') is not null
	drop procedure etl.map_lk_contacto
GO

create procedure etl.map_lk_contacto (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 28-04-2022
	-- Description:	Mapping LK Contactos (INFO)
	-- =============================================
	-- 28/04/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @msgLog varchar(500)

		-- Inicio
		set @msg = '>> Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Bajada
		select codigo as codigo, 
			id_cliente, id_pais,
			etl.fn_trim(nombre_ct) as nombre_ct,
			apellido_ct, titulo_ct, calle_ct, ciudad_ct, provincia_ct, 
			codpost_ct, telefono_ct, fax_ct, celular_ct, email_ct, acepta_part_ct
		into #datos
		from (
			select 1 as orden,
				'NA' as codigo, 
				0 as id_cliente,
				0 as id_pais,
				'Not Available' as nombre_ct,
				null as apellido_ct, 
				null as titulo_ct, 
				null as calle_ct, 
				null as ciudad_ct, 
				null as provincia_ct, 
				null as codpost_ct, 
				null as telefono_ct, 
				null as fax_ct, 
				null as celular_ct, 
				null as email_ct, 
				'0' as acepta_part_ct
			union all
			select distinct 2 as orden,
				st.cod_contacto,
				isnull(cl.id_cliente,0) as id_cliente,
				st.id_pais,
				st.nombre_ct, 
				st.apellido_ct,
				st.titulo_ct, 
				st.calle_ct,
				st.ciudad_ct,
				st.provincia_ct,
				st.codpost_ct,
				st.telefono_ct,
				st.fax_ct,
				st.celular_ct,
				st.email_ct,
				acepta_part_ct
			from (select guid_contacto as cod_contacto, 
				cuit, 
				etl.fn_LookupIDPais( cod_pais) id_pais, 
				nombre_ct, 
				apellido_ct,
				titulo_ct, calle_ct, ciudad_ct, provincia_ct, 
				cod_postal_ct as codpost_ct, telefono_ct,
				fax_ct, celular_ct, email_ct, acepta_part_ct
			from stg.ST_DGEN_LK_CONTACTOS) st
				left outer join (
					select * 
					from (select cuit, id_pais, id_cliente, cod_cliente,
								ROW_NUMBER ( )  OVER ( partition by cuit, id_pais 
							order by (case when id_categoria = 1 and id_estado = 1 then 1
								when id_categoria = 1  then 2
								when cod_customer <> 0 then 3
								when id_estado = 1 then 4
								else 5 end), id_cliente desc ) rk
						from dbo.DCTE_LK_CLIENTE
						where isnull(cuit,'') <> '') x
					where rk = 1) cl
					on cl.cuit = st.cuit and cl.id_pais = st.id_pais
		) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		/*
		select codigo
		from #datos 
		group by codigo
		having count(1) > 1

		select *
		from #datos
		where codigo = '0007034b-430e-1329-cfbf-b83335ea6e83'

		*/

		-- Validar Si hay Info
		if not exists( select 1 from #datos)
			begin
				set @msg = 'El Proceso de Carga no continúa debido a que no existen datos en la fuente.'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				exec etl.prc_reg_ctrl_mapping @msg, @idBatch
				return
			end

		begin transaction

		-- Insertar Nuevo
		insert into dbo.DGEN_LK_CONTACTOS (
			cod_contacto, id_cliente_ct, id_pais, nombre_ct, 
			apellidos_ct, titulo_ct, calle_ct, ciudad_ct, provincia_ct, codpost_ct, telefono_ct,
			fax_ct, celular_ct, email_ct, acepta_part_ct)
		select codigo, id_cliente, id_pais, nombre_ct, 
			apellido_ct, titulo_ct, calle_ct, ciudad_ct, provincia_ct, codpost_ct, telefono_ct,
			fax_ct, celular_ct, email_ct, acepta_part_ct
		from #datos a
		where not exists (select 1 
			from dbo.DGEN_LK_CONTACTOS b
			where b.cod_contacto = a.codigo)

		set @msg = '* Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK  
		set id_cliente_ct = b.id_cliente, 
			id_pais = b.id_pais, 
			nombre_ct = b.nombre_ct, 
			apellidos_ct = b.apellido_ct, 
			titulo_ct = b.titulo_ct, 
			calle_ct = b.calle_ct, 
			ciudad_ct = b.ciudad_ct, 
			provincia_ct = b.provincia_ct, 
			codpost_ct = b.codpost_ct, 
			telefono_ct = b.telefono_ct,
			fax_ct = b.fax_ct, 
			celular_ct = b.celular_ct, 
			email_ct = b.email_ct, 
			acepta_part_ct = b.acepta_part_ct
		from dbo.DGEN_LK_CONTACTOS LK, #datos b
		where b.codigo = LK.cod_contacto
			and (b.id_cliente <> LK.id_cliente_ct	
				or b.id_pais <> LK.id_pais	
				or isnull(b.nombre_ct,'') <> isnull(LK.nombre_ct,'')
				or isnull(b.apellido_ct,'') <> isnull(LK.apellidos_ct,'')
				or isnull(b.titulo_ct,'') <> isnull(LK.titulo_ct,'')
				or isnull(b.calle_ct,'') <> isnull(LK.calle_ct,'')
				or isnull(b.ciudad_ct,'') <> isnull(LK.ciudad_ct,'')
				or isnull(b.provincia_ct,'') <> isnull(LK.provincia_ct,'')
				or isnull(b.codpost_ct,'') <> isnull(LK.codpost_ct,'')
				or isnull(b.telefono_ct,'') <> isnull(LK.telefono_ct,'')
				or isnull(b.fax_ct,'') <> isnull(LK.fax_ct,'')
				or isnull(b.celular_ct,'') <> isnull(LK.celular_ct,'')
				or isnull(b.email_ct,'') <> isnull(LK.email_ct,'')
				or isnull(b.acepta_part_ct,'') <> isnull(LK.acepta_part_ct,'')
				)

		set @msg = '* Actualizados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Casos Que No Existen (Borrados en INFO)
		set @msg = '- Conctactos que no vienen mas de INFO:'; exec etl.prc_Logging @idBatch, @msg

		select id_contacto, cod_contacto, id_cliente_ct, id_pais, nombre_ct, apellidos_ct,
			cast(0 as numeric(15)) as id_contacto_new
		into #noExisten
		from dbo.DGEN_LK_CONTACTOS LK
		where LK.id_contacto > 0
			and not exists (select 1 
				from #datos a 
				where LK.cod_contacto = a.codigo)

		set @msg = '  Casos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Match - Reemplaza Nuevo ID
		update ne set id_contacto_new = lk.id_contacto
		from #noExisten ne,
			dbo.DGEN_LK_CONTACTOS lk
		where ne.id_contacto != lk.id_contacto
			and ne.cod_contacto != lk.cod_contacto
			and ne.id_cliente_ct = lk.id_cliente_ct
			and ne.id_pais = lk.id_pais
			and ne.nombre_ct = lk.nombre_ct
			and ne.apellidos_ct = lk.apellidos_ct
			and not exists (select 1 from #noExisten x
						where x.id_contacto = lk.id_contacto)

		set @msg = '  Match: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		update ft set id_contacto = ne.id_contacto_new
		from FT_OPORTUNIDADES ft,
			#noExisten ne
		where ft.id_contacto = ne.id_contacto

		set @msg = '  Update (FT OPP): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		update ft set id_contacto = ne.id_contacto_new
		from FT_FACTURACION_TS ft,
			#noExisten ne
		where ft.id_contacto = ne.id_contacto

		set @msg = '  Update (FT FAC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- delete 
		delete from LK 
		from dbo.DGEN_LK_CONTACTOS LK
		where exists (select 1 
			from #noExisten ne
			where LK.id_contacto = ne.id_contacto)

		set @msg = '* Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


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
-- exec etl.map_lk_contacto 0