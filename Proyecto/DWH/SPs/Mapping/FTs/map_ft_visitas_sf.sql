use MX_DWH
Go

if object_id ('etl.map_ft_visitas_sf', 'P') is not null
	drop procedure etl.map_ft_visitas_sf
GO

create procedure etl.map_ft_visitas_sf (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 22/04/2022
	-- Description:	Mapping FT Visitas SF
	-- =============================================
	-- 22/04/2022 - Version Inicial

	set dateformat 'YMD' 

	begin try
		set nocount on

		declare @msg varchar(500),@msgLog varchar(500), 
			@cant numeric(10), @titulo varchar(255),
			@linea numeric(10), @cod_visita varchar(255),
			@id_cliente numeric(10), @servicio varchar(2000),
			@distr_mail varchar(255)

		-- Inicio
		set @msg = '>> Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Fechas Ref
		select @msg = 'Fecha Ref. Desde: ' + desde + ' - Hasta: ' + hasta
		from (select convert(varchar, case when fc_min > fm_min then fc_min else fm_min end, 103) as desde,
				convert(varchar, case when fc_max > fm_max then fc_max else fm_max end, 103) as hasta			
			from (
				select min(cast(fecha_creacion as datetime)) fc_min, min(cast(fecha_modif as datetime)) fm_min,
					max(cast(fecha_creacion as datetime)) fc_max, max(cast(fecha_modif as datetime)) fm_max
				from stg.ST_FT_VISITAS_SF) x) y

		exec etl.prc_Logging @idBatch, @msg

		select @msg = 'Fecha Visitas. Desde: ' + desde + ' - Hasta: ' + hasta
		from (
			select convert(varchar, f_min, 103) as desde,
				convert(varchar, f_max, 103) as hasta			
			from (
				select min(cast(fecha as datetime)) f_min,
					max(cast(fecha as datetime)) f_max
				from stg.ST_FT_VISITAS_SF) x) y

		exec etl.prc_Logging @idBatch, @msg

		-------------------------------------------
		-- PREPROCESAMIENTO DATOS (PD)
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'PreProcesamiento de Datos:'

		begin transaction


		-- drop table #visitas_sf
		-- Bajada 
		select distinct vw.cod_visita, 
			vw.id_cliente, 
			case when vw.id_sucursal = -1 then 0 else vw.id_sucursal end as id_sucursal,
			case when vw.id_sucursal_orig = -1 then 0 else vw.id_sucursal_orig end as id_sucursal_orig,
			id_usuario,
			id_usuario_asig,
			vw.id_dia, 
			vw.id_tipo_visita, 
			vw.desc_visita,
			vw.id_mes, 
			0 as id_creador,
			vw.id_lista,
			0 as id_cc,
			isnull(id_posicion,0) as id_posicion,
			isnull(id_posicion_ua,0) as id_posicion_ua,
			vw.cod_sucursal,
			0 as kam,
			vw.mail_usuario,
			isnull(vw.id_dia_creacion,0) as id_dia_creacion, 
			isnull(vw.id_dia_modificacion,0) as id_dia_modificacion,
			isnull(substring(convert(varchar,hora_visita,108),1,5),'00:00') as hora_visita,
			servicio,
			0 as id_plan_comercial_opp
		into #visitas_sf
		from (
			select st.tech_guid as cod_visita, 
				etl.fn_LookupIdTipoVisita( tipo.cod_tipo_visita) as id_tipo_visita,
				case when st.cuit_cuenta is null then 0
								else isnull(cl.id_cliente,0) 
							end as id_cliente,
				etl.fn_LookupIdDGC( st.cod_sucursal ) as id_lista,
				etl.fn_LookupIdSucursal(st.cod_sucursal ) as id_sucursal_orig,
				isnull(etl.fn_getIDSucursalUsuario(usa.id_usuario, ldc.fecha), etl.fn_LookupIdSucursal(st.cod_sucursal)) as id_sucursal,
				isnull(us.id_usuario, etl.fn_LookupIdUsuario(null)) as id_usuario,
				isnull(usa.id_usuario, etl.fn_LookupIdUsuario(null)) as id_usuario_asig,
				st.mail_creador,
				st.mail_usuario,
				isnull(ld.id_dia,0) as id_dia, 
				isnull(ld.id_mes,0) as id_mes,
				isnull(isnull(isnull(isnull(st.tema_principal, st.objetivo),comentarios), st.resultado),st.asunto) as desc_visita,
				us.id_posicion,
				usa.id_posicion as id_posicion_ua,
				isnull(ldc.id_dia,0) as id_dia_creacion,
				isnull(ldm.id_dia,0) as id_dia_modificacion,
				st.cod_sucursal, 
				cast(st.hora_venc as datetime) as hora_visita,
				st.servicio
			-- select count(1)
			from stg.ST_FT_VISITAS_SF st
				left outer join dbo.DCTE_LK_CLIENTE cl
					on cl.guid = st.guid_cuenta
				left outer join dbo.DTPO_LK_DIAS ld
					on ld.fecha = etl.fn_truncFecha(st.fecha)
				left outer join dbo.DGEN_LK_USUARIO us
					on us.mail = etl.fn_trim(lower(st.mail_creador))
				left outer join dbo.DGEN_LK_USUARIO usa
					on usa.mail = etl.fn_trim(lower(st.mail_usuario))
				left outer join dbo.DVIS_LK_TIPO_VISITA tipo
					on tipo.desc_tipo_visita = st.subtipo
				left outer join dbo.DTPO_LK_DIAS ldc
					on ldc.fecha = etl.fn_truncFecha(st.fecha_creacion)
				left outer join dbo.DTPO_LK_DIAS ldm
					on ldm.fecha = etl.fn_truncFecha(st.fecha_modif)
				) vw

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		/*
		select cod_visita
		from #visitas_sf  
		group by cod_visita
		having count(1)> 1

		select * from #visitas_sf   where cod_visita = '1c03e98b-6cc4-4643-9246-fa2cd0255e55'
		*/

		-- Duplicados
		;WITH cte AS
		(SELECT *, ROW_NUMBER() OVER (PARTITION BY v.cod_visita  ORDER BY (SELECT 0)) AS Duplicado
			FROM #visitas_sf v
		)
		DELETE
		FROM cte WHERE Duplicado > 1;

		set @msg = 'Borra Duplicados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		---- Visitas x Servicios
		create unique index #visitas_sf_uk on #visitas_sf  (cod_visita)

		/*
		update st
		set id_plan_comercial_opp = etl.fn_LookupIdPlanComercial (vw.cod_plan_comercial_opp)
		from #visitas_sf st,
				(select cod_visita, max(isnull(plan_comercial,'NA')) as cod_plan_comercial_opp
			from dbo.VW_REL_VISITAS_OPP
			where plan_comercial is not null
			group by cod_visita) vw
		where st.cod_visita = vw.cod_visita

		set @msg = 'Update Plan Comercial OPP: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		*/

		update ft set id_sucursal = case when id_sucursal < 0 then 0 else id_sucursal end
		-- select * 
		from #visitas_sf ft
		where id_lista > 0

		-- Sin Servicio
		select cod_visita,  
			etl.fn_LookupIdTipoServDetalle(servicio) as id_tipo_servicio_visita
		into #visitas_sf_serv
		from #visitas_sf a
		where servicio is null 

		set @cant = @@rowcount
		
		-- Procesos de la Secuencia
		declare lc_cursor cursor for	
		select cod_visita, replace(servicio, ';', ',') as servs
		-- select distinct servicio
		from #visitas_sf a
		where servicio is not null

		open lc_cursor
		fetch next from lc_cursor into @cod_visita, @servicio
		while  @@fetch_status = 0
			begin

				insert into #visitas_sf_serv (cod_visita, id_tipo_servicio_visita)
				select @cod_visita, etl.fn_LookupIdTipoServDetalle(b.bi) as id_tipo_servicio_visita
				from (
					select distinct * 
					from etl.fn_getTablaLista (@servicio)) a
				 left outer join 
					(select 'Temporary staffing' as sf, 'MX_TEMP' as bi union all
					select 'Outsourcing', 'MX_BPO_IND' union all
					select 'Permanent placement', 'MX_PERM' union all
					select 'RPO', 'MX_RPO_OUT' union all
					select 'Consulting', 'MX_CONSULTING' union all
					select 'Training', 'MX_TRAINING' union all
					select 'Payroll', 'MX_PAY') b on a.valor = b.sf

				set @cant = @cant +  @@rowcount

				--next 
				fetch next from lc_cursor into @cod_visita, @servicio
			end
		close lc_cursor
		deallocate lc_cursor

		set @msg = 'Bajada Visitas x Servicio: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg

		-------------------------------------------
		-- CARGA
		-------------------------------------------
		exec etl.prc_Logging @idBatch, 'Carga DW:'

		-- Insertar
		insert into dbo.FT_VISITAS ( id_cliente, cod_visita,
			id_sucursal, id_dia, id_tipo_visita, 
			desc_visita, id_mes, id_creador, id_lista, id_cc, id_usuario,
			id_posicion, id_dia_creacion, id_dia_modificacion,
			id_usuario_asig, id_sucursal_orig, hora_visita, id_posicion_ua,
			id_plan_comercial_opp)
		select id_cliente, cod_visita,
			id_sucursal, id_dia, id_tipo_visita, 
			desc_visita, id_mes, id_creador, id_lista, id_cc, a.id_usuario,
			id_posicion, id_dia_creacion, id_dia_modificacion,
			id_usuario_asig, id_sucursal_orig, hora_visita, id_posicion_ua,
			id_plan_comercial_opp
		from #visitas_sf a
		where not exists (select 1 
						from dbo.FT_VISITAS b
						where b.cod_visita = a.cod_visita)
		
		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Actualizar
		update FT
		set id_sucursal	= PD.id_sucursal,
			id_dia = PD.id_dia,
			id_tipo_visita = PD.id_tipo_visita,
			desc_visita = PD.desc_visita,
			id_mes = PD.id_mes,
			id_creador = PD.id_creador,
			id_lista = PD.id_lista,
			id_cc = PD.id_cc,
			id_usuario = PD.id_usuario,
			id_cliente = PD.id_cliente,
			id_dia_creacion = PD.id_dia_creacion,
			id_dia_modificacion = PD.id_dia_modificacion,
			id_usuario_asig = PD.id_usuario_asig,
			id_sucursal_orig = PD.id_sucursal_orig,
			hora_visita = PD.hora_visita,
			id_plan_comercial_opp = PD.id_plan_comercial_opp
		-- select *
		from dbo.FT_VISITAS FT,
			(select id_cliente, cod_visita,
				id_sucursal, id_dia, id_tipo_visita, 
				desc_visita, id_mes, id_creador, id_lista, id_cc, id_usuario,
				id_posicion, id_posicion_ua, id_dia_creacion, id_dia_modificacion,
				id_usuario_asig, id_sucursal_orig, hora_visita, id_plan_comercial_opp
			from #visitas_sf a) PD
		where FT.cod_visita = PD.cod_visita
			and (FT.id_cliente <> PD.id_cliente
				or FT.id_sucursal <> PD.id_sucursal
				or FT.id_dia <> PD.id_dia
				or FT.id_tipo_visita <> PD.id_tipo_visita
				or etl.fn_trim(isnull(FT.desc_visita,'')) <> isnull(PD.desc_visita,'')
				or FT.id_mes <> PD.id_mes
				or FT.id_creador <> PD.id_creador
				or FT.id_lista <> PD.id_lista
				or FT.id_usuario <> PD.id_usuario
				or FT.id_cc <> PD.id_cc
				or FT.id_dia_creacion <> PD.id_dia_creacion
				or FT.id_dia_modificacion <> PD.id_dia_modificacion
				or FT.id_usuario_asig <> PD.id_usuario_asig
				or FT.id_sucursal_orig <> PD.id_sucursal_orig
				or FT.hora_visita <> PD.hora_visita
				or FT.id_plan_comercial_opp <> PD.id_plan_comercial_opp)

		set @msg = 'Actualizados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Visitas Por Servicios
		-- Borrados
		delete FT
		from dbo.FT_VISITAS_SERVICIO FT
		where  exists (select 1 
						from #visitas_sf  x
						where x.cod_visita = FT.cod_visita)
		
		set @msg = 'Borrados (Por Servicio): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Insertar
		insert into dbo.FT_VISITAS_SERVICIO ( id_cliente, cod_visita,
			id_sucursal, id_dia, id_tipo_visita, 
			desc_visita, id_mes, id_creador, id_lista, id_cc, id_usuario,
			id_posicion, id_dia_creacion, id_dia_modificacion,
			id_usuario_asig, id_sucursal_orig, hora_visita, id_posicion_ua, id_tipo_servicio_visita)
		select distinct a.id_cliente, a.cod_visita,
			id_sucursal, id_dia, id_tipo_visita, 
			desc_visita, id_mes, id_creador, id_lista, id_cc, a.id_usuario,
			id_posicion, id_dia_creacion, id_dia_modificacion,
			id_usuario_asig, id_sucursal_orig, hora_visita, id_posicion_ua, b.id_tipo_servicio_visita
		from #visitas_sf a,
			#visitas_sf_serv b
		where a.cod_visita = b.cod_visita
		
		set @msg = 'Insertados (Por Servicio): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
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
-- exec etl. map_ft_visitas_sf 0