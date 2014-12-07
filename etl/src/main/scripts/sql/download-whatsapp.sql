SELECT '"' || apps.subs_id || '"|"' || subs.serv_no || '"|"' || SUM(apps.tot_vol) FROM IA.T_IA_APP_TYPE_CNT_VOL_D apps LEFT JOIN CKM.OFR_SUBS_HIS_D subs ON (apps.subs_id=subs.subs_id AND pay_mode_cd = 16501 AND data_date IN ('20141130', '20141201', '20141202') ) WHERE app_type=859 AND apps.data_day IN ('20141130', '20141201', '20141202') AND apps.subs_id NOT IN ( SELECT DISTINCT(subs_id) FROM IA.T_IA_APP_TYPE_CNT_VOL_D WHERE app_type=859 and data_day < 20141130) GROUP BY apps.subs_id, subs.serv_no HAVING SUM(apps.tot_vol)>10240