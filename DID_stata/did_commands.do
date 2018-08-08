gen time = week>=16

* simple DID *
diff weekly_stream_count, t(treated) p(time) robust

* with covariates *
diff weekly_stream_count, t(treated) p(time) cov(age_bin_adult age_bin_senior age_bin_teens age_bin_youngadult male premium) robust report

* with selected covariates *
diff weekly_stream_count, t(treated) p(time) cov(male premium) robust report

* quantile DID*
diff weekly_stream_count, t(treated) p(time) cov(male premium) qdid(0.25) robust
diff weekly_stream_count, t(treated) p(time) cov(male premium) qdid(0.5) robust
diff weekly_stream_count, t(treated) p(time) cov(male premium) qdid(0.75) robust


* for treatment comparison *
* with covariates *
diff weekly_stream_count, t(is_playlist) p(time) cov(male premium) robust report

diff weekly_stream_count, t(is_playlist) p(time) cov(male premium) qdid(0.25) robust
diff weekly_stream_count, t(is_playlist) p(time) cov(male premium) qdid(0.5) robust
diff weekly_stream_count, t(is_playlist) p(time) cov(male premium) qdid(0.75) robust
