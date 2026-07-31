import json, sys
from playwright.sync_api import sync_playwright

B = "http://127.0.0.1:8090"
out = {}
with sync_playwright() as p:
    br = p.chromium.launch()
    pg = br.new_page()
    errs, reqs = [], []
    pg.on("console", lambda m: errs.append(m.type + ": " + m.text) if m.type == "error" else None)
    pg.on("pageerror", lambda e: errs.append("pageerror: " + str(e)))
    pg.on("response", lambda r: reqs.append((r.url.split("/api/")[-1].split("?")[0], r.status))
          if "/api/" in r.url else None)
    pg.goto(B, wait_until="networkidle")
    pg.wait_for_function("typeof meta!=='undefined' && meta && meta.sensors", timeout=15000)
    pg.select_option("#sensor", "DEMO-Directional")
    pg.wait_for_timeout(2500)
    out["after_sensor_select"] = pg.evaluate("layerMode")

    # zoom time to 6 hours (well under PSD_THRESHOLD), full band
    pg.evaluate("()=>{const c=(meta.tmin+meta.tmax)/2; view={t0:c-3*3600,t1:c+3*3600};"
                "clampTimeView(); layoutTimeSlider(); requestData();}")
    pg.wait_for_timeout(2000)
    out["time_zoom_6h_fullband"] = pg.evaluate("layerMode")
    out["psd_band_MHz"] = pg.evaluate("psdView?[psdView.fmin/1e6,psdView.fmax/1e6]:null")

    # frequency zoom: narrow to one 12 MHz channel around 3560 MHz
    pg.evaluate("()=>{viewF={f0:3554e6,f1:3566e6}; onFreqChange();}")
    pg.wait_for_timeout(2500)
    out["freq_zoom_one_channel"] = pg.evaluate("layerMode")
    out["pfp_channel_MHz"] = pg.evaluate("pfpView?pfpView.freq/1e6:null")
    out["nearestChannel_MHz"] = pg.evaluate("nearestChannelHz()/1e6")

    # frequency zoom over a band with no PFP channel -> must stay on PSD, narrowed
    pg.evaluate("()=>{viewF={f0:3594e6,f1:3606e6}; onFreqChange();}")
    pg.wait_for_timeout(2500)
    out["freq_zoom_no_pfp_channel"] = pg.evaluate("layerMode")
    out["psd_band_narrowed_MHz"] = pg.evaluate("psdView?[psdView.fmin/1e6,psdView.fmax/1e6]:null")

    # IQ mode
    caps = pg.evaluate("()=>[...document.querySelectorAll('#source option')].map(o=>o.value)")
    iq = [c for c in caps if c.startswith("iq:")]
    if iq:
        pg.select_option("#source", iq[0])
        pg.wait_for_timeout(3000)
        out["iq_mode"] = pg.evaluate("layerMode")
        out["iq_tile"] = pg.evaluate("iqView?{cols:iqView.cols,nf:iqView.nf,level:iqView.level}:null")
    out["console_errors"] = errs
    out["bad_api"] = [r for r in reqs if r[1] >= 400]
    br.close()
print(json.dumps(out, indent=1))
