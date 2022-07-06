use MX_DWH
Go

if object_id ('etl.map_ft_pl_opportunities', 'P') is not null
	drop procedure etl.map_ft_pl_opportunities
GO

create procedure etl.map_ft_pl_opportunities (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	07-06-2022
	-- Description:	Oportunidades SF (Varios Paises)
	-- =============================================
	-- 07/06/2022 - Version Inicial

	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- drop table #datos
		select opportunityid, 
			guid_opp, 
			cod_pais, 
			pais, 
			max(Etl.fn_TruncFecha( fecha_creacion) ) as fecha_creacion, 
			max(Etl.fn_TruncFecha( fecha_modificacion) ) as fecha_modificacion, 
			max(Etl.fn_TruncFecha( fecha_cierre) ) as fecha_cierre, 
			max(Etl.fn_TruncFecha( ult_modif_stage) ) as ult_modif_stage, 
			max(nombre_opp) as nombre_opp, 
			max(servicio) as servicio, 
			max(subservicio) as subservicio, 
			max(propietario_opp) as propietario_opp, 
			max(cod_moneda) as cod_moneda, 
			max(isnull(mb_previsto_porc,0)) as mb_previsto_porc, 
			max(isnull(mb_previsto_imp, 0)) as mb_previsto_imp,
			max(isnull(importe, 0)) as importe,
			max(isnull(cantidad_opp, 0)) as cantidad_opp,
			max(isnull(cast(venta_potencial as numeric(24,3)),0)) as venta_potencial, 
			max(tipo_opp) as tipo_opp, 
			max(etapa_opp) as etapa_opp, 
			max(tipo_reg_opp) as tipo_reg_opp, 
			max(isnull(propuesta_enviada, 0)) as propuesta_enviada,
			max(Etl.fn_TruncFecha( fecha_envio_propuesta ) ) as fecha_envio_propuesta, 
			max(isnull(probabilidad, 0)) as probabilidad, 
			max(isnull(opp_borrada, 0)) as opp_borrada,
			max(isnull(opp_ganada, 0)) as opp_ganada,
			max(segmento_local) as segmento_local, 
			max(razon_gp) as razon_gp, 
			max(razon_gp_comment) as razon_gp_comment, 
			max(nombre_cuenta) as nombre_cuenta, 
			max(cod_sucursal) as cod_sucursal, 
			max(sucursal) as sucursal 
		into #datos
		from stg.ST_FT_PL_OPPORTUNITIES st
		group by opportunityid, guid_opp, cod_pais, pais

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg

		create unique index #datos_uk on #datos (opportunityid)
		
		begin transaction

			insert into dbo.FT_PL_OPPORTUNITIES (opportunityid, guid_opp, cod_pais, pais, fecha_creacion, fecha_modificacion, 
				fecha_cierre, ult_modif_stage, nombre_opp, servicio, subservicio, 
				propietario_opp, cod_moneda, mb_previsto_porc, mb_previsto_imp, importe, 
				cantidad_opp, venta_potencial, tipo_opp, etapa_opp, tipo_reg_opp, 
				propuesta_enviada, fecha_envio_propuesta, probabilidad, opp_borrada, opp_ganada, 
				segmento_local, razon_gp, razon_gp_comment, nombre_cuenta, cod_sucursal, 
				sucursal)	
			select opportunityid, guid_opp, cod_pais, pais, fecha_creacion, fecha_modificacion, 
				fecha_cierre, ult_modif_stage, nombre_opp, servicio, subservicio, 
				propietario_opp, cod_moneda, mb_previsto_porc, mb_previsto_imp, importe, 
				cantidad_opp, venta_potencial, tipo_opp, etapa_opp, tipo_reg_opp, 
				propuesta_enviada, fecha_envio_propuesta, probabilidad, opp_borrada, opp_ganada, 
				segmento_local, razon_gp, razon_gp_comment, nombre_cuenta, cod_sucursal, 
				sucursal
			from #datos st
			where not exists (select 1 from dbo.FT_PL_OPPORTUNITIES ft
				where ft.opportunityid = st.opportunityid)

			set @cant = @@rowcount
			set @msg = 'Insertados FT: ' + convert(varchar, @cant)exec etl.prc_Logging @idBatch, @msg

			update ft
			set guid_opp = st.guid_opp, 
				cod_pais = st.cod_pais, 
				pais = st.pais, 
				fecha_creacion = st.fecha_creacion, 
				fecha_modificacion = st.fecha_modificacion, 
				fecha_cierre = st.fecha_cierre, 
				ult_modif_stage = st.ult_modif_stage, 
				nombre_opp = st.nombre_opp, 
				servicio = st.servicio, 
				subservicio = st.subservicio, 
				propietario_opp = st.propietario_opp, 
				cod_moneda = st.cod_moneda, 
				mb_previsto_porc = st.mb_previsto_porc, 
				mb_previsto_imp = st.mb_previsto_imp,
				importe = st.importe, 
				cantidad_opp = st.cantidad_opp, 
				venta_potencial = st.venta_potencial,
				tipo_opp = st.tipo_opp, 
				etapa_opp = st.etapa_opp, 
				tipo_reg_opp = st.tipo_reg_opp, 
				propuesta_enviada = st.propuesta_enviada,
				fecha_envio_propuesta = st.fecha_envio_propuesta, 
				probabilidad = st.probabilidad,
				opp_borrada = st.opp_borrada,
				opp_ganada = st.opp_ganada, 
				segmento_local = st.segmento_local, 
				razon_gp = st.razon_gp, 
				razon_gp_comment = st.razon_gp_comment, 
				nombre_cuenta = st.nombre_cuenta, 
				cod_sucursal = st.cod_sucursal, 
				sucursal = st.sucursal,
				fec_modificacion = getdate()
			from dbo.FT_PL_OPPORTUNITIES ft,
				#datos st
			where ft.opportunityid = st.opportunityid
				and (ft.guid_opp != st.guid_opp
					or ft.cod_pais != st.cod_pais 
					or ft.pais != st.pais
					or isnull(ft.fecha_creacion, cast('1900-01-01' as datetime)) != isnull(st.fecha_creacion, cast('1900-01-01' as datetime))
					or isnull(ft.fecha_modificacion, cast('1900-01-01' as datetime)) != isnull(st.fecha_modificacion, cast('1900-01-01' as datetime))
					or isnull(ft.fecha_cierre, cast('1900-01-01' as datetime)) != isnull(st.fecha_cierre, cast('1900-01-01' as datetime))
					or isnull(ft.ult_modif_stage, cast('1900-01-01' as datetime)) != isnull(st.ult_modif_stage, cast('1900-01-01' as datetime))
					or isnull(ft.nombre_opp,'') != isnull(st.nombre_opp,'')
					or isnull(ft.servicio,'') != isnull(st.servicio,'')
					or isnull(ft.subservicio,'') != isnull(st.subservicio,'')
					or isnull(ft.propietario_opp,'') != isnull(st.propietario_opp,'')
					or isnull(ft.cod_moneda,'') != isnull(st.cod_moneda,'')
					or ft.mb_previsto_porc != st.mb_previsto_porc
					or ft.mb_previsto_imp != st.mb_previsto_imp
					or ft.importe != st.importe
					or ft.cantidad_opp != st.cantidad_opp
					or ft.venta_potencial != st.venta_potencial
					or isnull(ft.tipo_opp,'') != isnull(st.tipo_opp,'')
					or isnull(ft.etapa_opp,'') != isnull(st.etapa_opp,'')
					or isnull(ft.tipo_reg_opp,'') != isnull(st.tipo_reg_opp,'')
					or ft.propuesta_enviada != st.propuesta_enviada
					or isnull(ft.fecha_envio_propuesta, cast('1900-01-01' as datetime)) != isnull(st.fecha_envio_propuesta, cast('1900-01-01' as datetime))
					or ft.probabilidad != st.probabilidad
					or ft.opp_borrada != st.opp_borrada
					or ft.opp_ganada != st.opp_ganada
					or isnull(ft.segmento_local,'-1') != isnull(st.segmento_local,'-1')
					or isnull(ft.razon_gp,'') != isnull(st.razon_gp,'')
					or isnull(ft.razon_gp_comment,'') != isnull(st.razon_gp_comment,'')
					or isnull(ft.nombre_cuenta,'') != isnull(st.nombre_cuenta,'')
					or isnull(ft.cod_sucursal,'') != isnull(st.cod_sucursal,'')
					or isnull(ft.sucursal,'') != isnull(st.sucursal,'')
					)

			set @msg = 'Actualizados: ' + convert(varchar, @@rowcount)exec etl.prc_Logging @idBatch, @msg


		commit transaction

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

		-- error
		return -1

	end catch
end
GO
-- exec etl.map_ft_pl_opportunities 0 