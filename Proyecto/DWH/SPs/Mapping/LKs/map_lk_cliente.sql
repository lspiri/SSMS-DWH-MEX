use MX_DWH
Go

if object_id ('etl.map_lk_cliente', 'P') is not null
	drop procedure etl.map_lk_cliente
GO

create procedure etl.map_lk_cliente (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21/03/2022
	-- Description:	Mapping LK Cliente
	-- =============================================
	-- 22/04/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction
	
		-- drop table #datos
		-- drop table #volumen

		-- Volumen (Cant PI)
		select 'NA' as cod_cliente, 
			'NA' as cod_volumen,
			0 cantidad_pi
		into #volumen
		from dbo.ODS_ANUAL_CANT_PI x
		where anio = year(getdate())

		create unique index #volumen_uk on #volumen (cod_cliente asc)

		-- Int Sales
		update tmp 
		set int_sales = 'N'
		from stg.ST_DCTE_LK_CLIENTE tmp
		
		-- Bajada
		select codigo, razon_social, cod_customer, dom_calle, 
			dom_numero, dom_piso, localidad, 
			codigo_postal, telefono, cuit, id_estado,
			id_actividad, id_tipo_cliente, id_provincia,
			id_volumen, id_tipo_lista, cantidad_pi, id_categoria,
			id_lista, prospect_pricing, id_segmento_mercado, id_pais,
			id_segmento_local, id_segmento_global,
			guid
		into #datos
		from (
			select -1 as orden,
				'-1' as codigo, 
				'Not Exists' as razon_social,
				0 as cod_customer,
				'' as dom_calle, 
				'' as dom_numero, 
				'' as dom_piso, 
				'' as localidad, 
				'' as codigo_postal, 
				'' as telefono, 
				'' as cuit, 
				0 as cantidad_pi,
				etl.fn_LookupIdEstado(null) id_estado,
				etl.fn_LookupIdActividad(null) id_actividad, 
				etl.fn_LookupIdTipoCliente(null) id_tipo_cliente, 
				etl.fn_LookupIdProvincia(null) id_provincia,
				etl.fn_LookupIdVolumenCte(null) as id_volumen, 
				etl.fn_LookupIdTipoListaCte(null) as id_tipo_lista,
				etl.fn_LookupIdCategoria('PROS') as id_categoria,
				etl.fn_LookupIdDGC(null) id_lista,
				0 as prospect_pricing,
				etl.fn_LookupIdSegmentoMercado(null) as id_segmento_mercado,
				etl.fn_LookupIdPais(null) as id_pais,
				etl.fn_LookupIdSegmentoLocal(null) as id_segmento_local,
				etl.fn_LookupIdSegmentoGlobal(null) as id_segmento_global,
				cast(null as varchar(50)) as guid
			union all
			select 1 as orden,
				'NA' as codigo, 
				'Not Available' as razon_social,
				0 as cod_customer,
				'' as dom_calle, 
				'' as dom_numero, 
				'' as dom_piso, 
				'' as localidad, 
				'' as codigo_postal, 
				'' as telefono, 
				'' as cuit, 
				0 as cantidad_pi,
				etl.fn_LookupIdEstado(null) id_estado,
				etl.fn_LookupIdActividad(null) id_actividad, 
				etl.fn_LookupIdTipoCliente(null) id_tipo_cliente, 
				etl.fn_LookupIdProvincia(null) id_provincia,
				etl.fn_LookupIdVolumenCte(null) as id_volumen, 
				etl.fn_LookupIdTipoListaCte(null) as id_tipo_lista,
				etl.fn_LookupIdCategoria('PROS') as id_categoria,
				etl.fn_LookupIdDGC(null) id_lista,
				0 as prospect_pricing,
				etl.fn_LookupIdSegmentoMercado(null) as id_segmento_mercado,
				etl.fn_LookupIdPais(null) as id_pais,
				etl.fn_LookupIdSegmentoLocal(null) as id_segmento_local,
				etl.fn_LookupIdSegmentoGlobal(null) as id_segmento_global,
				cast(null as varchar(50)) as guid
			union all
			select 2 as orden, 
				convert(varchar,a.cod_cliente), 
				etl.fn_trim(a.razon_social),
				isnull(a.cod_customer,0) as cod_customer,
				etl.fn_trim(isnull(a.dom_calle, '')),
				substring(isnull(etl.fn_trim(a.dom_numero), ''),1,20),
				isnull(a.dom_piso, ''), 
				isnull(a.localidad, ''), 
				isnull(a.codigo_postal, ''), 
				isnull(a.telefono, ''), 
				case when a.cuit_ok is not null then
						a.cuit_ok
					 else	
						a.cuit
				end as cuit,
				isnull(c.cantidad_pi,0) as cantidad_pi,
				etl.fn_LookupIdEstado(a.estado) id_estado,				
				etl.fn_LookupIdActividad( isnull(a.cod_actividad_det, a.cod_actividad) ) id_actividad,
				etl.fn_LookupIdTipoCliente('NA') as id_tipo_cliente,
				etl.fn_LookupIdProvincia(a.provincia) id_provincia,
				etl.fn_LookupIdVolumenCte(
						case when a.segmento is not null and a.segmento <> 'NA' then
							etl.fn_getClasifClienteCRM(a.segmento)
						else
							c.cod_volumen
						end) as id_volumen,
				case isnull(a.int_sales,'')
					when 'S' then
						etl.fn_LookupIdTipoListaCte('IS')
					else 
						etl.fn_LookupIdTipoListaCte('NIS')
				end id_tipo_lista,
				etl.fn_LookupIdCategoria(case 
					when a.prospecto = '1' then
						'PROS'
					when a.prospecto = '0' then
						'CLTE'
					when a.prospecto is not null then
						'OTRO' 
					else
						null
				end) as id_categoria,
				etl.fn_LookupIdDGC(etl.fn_getCodListaDGCDesc(a.nombre_lista_dgc)) id_lista,
				case when prospecto = 1 and a.cod_cliente = a.fiscal_code then 1 else 0 end prospect_pricing,
				etl.fn_LookupIdSegmentoMercado(a.cod_segmento_mercado) as id_segmento_mercado,
				etl.fn_LookupIdPais(cod_pais) as id_pais,
				etl.fn_LookupIdSegmentoLocal(cod_segmento_local) as id_segmento_local,
				etl.fn_LookupIdSegmentoGlobal(cod_segmento_global) as id_segmento_global,
				guid
			from stg.ST_DCTE_LK_CLIENTE a
				left outer join #volumen c on (c.cod_cliente = a.cod_cliente)
			where a.cod_cliente is not null) x
		order by x.orden, x.codigo

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (codigo asc)

		-- Insertar Nuevo
		insert into dbo.DCTE_LK_CLIENTE (
			cod_cliente, cod_customer, razon_social, dom_calle, 
			dom_numero, dom_piso, localidad, 
			codigo_postal, telefono, cuit, id_estado,
			id_actividad, id_tipo_cliente, id_provincia,
			id_volumen, id_tipo_lista, cantidad_pi, id_categoria,
			id_lista, id_segmento_mercado, id_pais, id_segmento_local,
			id_segmento_global, guid)
		select codigo, cod_customer, razon_social, dom_calle, 
			dom_numero, dom_piso, localidad, 
			codigo_postal, telefono, cuit, id_estado,
			id_actividad, id_tipo_cliente, id_provincia,
			id_volumen, id_tipo_lista, cantidad_pi, id_categoria,
			id_lista, id_segmento_mercado, id_pais, id_segmento_local,
			id_segmento_global, guid
		from #datos a
		where not exists (select 1 
			from dbo.DCTE_LK_CLIENTE b
			where b.cod_cliente = a.codigo)

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar Descripcion
		update LK
		set razon_social = b.razon_social, 
			cod_customer = b.cod_customer,
			dom_calle = b.dom_calle, 
			dom_numero = isnull(b.dom_numero,''), 
			dom_piso = b.dom_piso, 
			localidad = b.localidad, 
			codigo_postal = b.codigo_postal, 
			telefono = b.telefono, 
			cuit = b.cuit, 
			id_estado = b.id_estado,
			id_actividad = b.id_actividad, 
			id_tipo_cliente = b.id_tipo_cliente, 
			id_provincia = b.id_provincia,
			id_volumen = b.id_volumen, 
			id_tipo_lista = b.id_tipo_lista,
			cantidad_pi = b.cantidad_pi,
			id_categoria = b.id_categoria,
			id_lista = b.id_lista,
			id_segmento_mercado = b.id_segmento_mercado,
			id_pais = b.id_pais,
			id_segmento_local = b.id_segmento_local,
			id_segmento_global = b.id_segmento_global,
			guid = b.guid
		from dbo.DCTE_LK_CLIENTE LK,
			#datos b
		where b.codigo = LK.cod_cliente
			and (isnull(b.razon_social,'') <> isnull(LK.razon_social,'')
				or b.cod_customer <> isnull(LK.cod_customer,0)
				or isnull(b.dom_calle,'') <> isnull(LK.dom_calle,'')
				or isnull(b.dom_numero,'') <> isnull(LK.dom_numero,'')
				or isnull(b.dom_piso,'') <> isnull(LK.dom_piso,'')
				or isnull(b.localidad,'') <> isnull(LK.localidad,'')
				or isnull(b.codigo_postal,'') <> isnull(LK.codigo_postal,'')
				or isnull(b.telefono,'') <> isnull(LK.telefono,'')
				or isnull(b.cuit,'') <> isnull(LK.cuit,'')
				or isnull(b.guid,'') <> isnull(LK.guid,'')
				or b.id_estado <> LK.id_estado
				or b.id_actividad <> LK.id_actividad
				or b.id_tipo_cliente <> LK.id_tipo_cliente
				or b.id_provincia <> LK.id_provincia
				or b.id_volumen <> LK.id_volumen
				or b.id_tipo_lista <> LK.id_tipo_lista
				or b.cantidad_pi <> isnull(LK.cantidad_pi,-1)
				or b.id_categoria <>  LK.id_categoria
				or b.id_lista <> LK.id_lista
				or b.id_segmento_mercado <> LK.id_segmento_mercado
				or b.id_pais <> LK.id_pais
				or b.id_segmento_local <> LK.id_segmento_local
				or b.id_segmento_global <> LK.id_segmento_global
				)

		set @msg = 'Actualizados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Pone Inactivos a Cliente que no aparecen
		Update LK
		set id_estado = inac.id_estado_inactivo
		from dbo.DCTE_LK_CLIENTE LK,
			(select etl.fn_LookupIdEstado ('I') id_estado_inactivo) inac
		where id_estado <> inac.id_estado_inactivo
			and not exists (select 1 
			from #datos b
			where b.codigo = LK.cod_cliente)
		

		set @msg = 'Actualizados (Inactivos): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
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
-- exec etl.map_lk_cliente 0