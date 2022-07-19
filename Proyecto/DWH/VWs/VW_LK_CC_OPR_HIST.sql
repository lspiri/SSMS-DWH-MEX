USE MX_DWH
Go

if object_id ('dbo.VW_LK_CC_OPR_HIST', 'V') is not null
	drop view dbo.VW_LK_CC_OPR_HIST
Go

create view dbo.VW_LK_CC_OPR_HIST as
select vw.ID_MES,
	vw.ID_CC, vw.COD_CC, vw.DESC_CC, g.COD_AGRUPADOR, vw.ID_AGRUPADOR, vw.REGION_OPR, vw.DOP_OPR, vw.ID_ZONA,
	vw.COD_ZONA, vw.DESC_ZONA, vw.ORDEN_PRESENT, vw.DIR_PRORRATEO, vw.ALLOCATION, vw.ORIGEN
from (
	select m.id_mes as ID_MES,
		vw.ID_CC, vw.COD_CC, vw.DESC_CC, vw.ID_AGRUPADOR, vw.REGION_OPR, vw.DOP_OPR, vw.ID_ZONA,
		vw.COD_ZONA, vw.DESC_ZONA, vw.ORDEN_PRESENT, vw.DIR_PRORRATEO, vw.ALLOCATION, vw.ORIGEN,
		rank() over (partition by m.id_mes,  vw.ID_CC order by case when vw.ORIGEN like 'ODS%' then 1 else 2 end, vw.id_agrupador desc) as rk
	from (
		select cast(ods.periodo as numeric(10)) as id_mes,
			lk.id_cc,
			lk.cod_cc,
			lk.desc_cc,
			isnull(g2.id_agrupador, lk.id_agrupador) as id_agrupador,
			lk.region_opr,
			lk.dop_opr,
			isnull(isnull(z2.id_zona, z.id_zona),0) as id_zona,
			isnull(isnull(ods.cod_zona, z.cod_zona),'NA') cod_zona,
			isnull(isnull(z2.desc_zona, z.desc_zona), lk.region_opr_2) as desc_zona,
			isnull(ods.orden_present,0) as orden_present,
			isnull(ods.dir_prorrateo, 'NO') as dir_prorrateo,
			ods.allocation,
			isnull('ODS (' + ods.origen + ')', 'LK_CC_OPR') as origen
		from dbo.DGEN_LK_CC_OPR lk
			left outer join dbo.ODS_OPR_AGRUPADOR_ZONA ods
				on ods.cod_cc = lk.cod_cc
					left outer join dbo.DGEN_LK_AGRUPADOR_CC g2
						on g2.cod_agrupador = ods.cod_agrupador
					left outer join dbo.DGEO_LK_ZONA z2
				on ods.cod_zona = z2.cod_zona
			left outer join dbo.DGEO_LK_ZONA z
				on lk.region_opr_2 in (z.desc_zona, z.cod_zona) 
			) vw,
		(select id_mes
		from dbo.DTPO_LK_MESES a
		where a.anio >= 2022
			--and a.id_mes <= year(getdate()) * 100 + month(getdate()) 
			and a.id_mes <= year(getdate()) * 100 + 12
			) m
	where m.id_mes = vw.id_mes
		or (vw.id_mes is null and m.id_mes > 0) ) vw
		left outer join dbo.DGEN_LK_AGRUPADOR_CC g
			on g.id_agrupador = vw.id_agrupador
where vw.rk = 1