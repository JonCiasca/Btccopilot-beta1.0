From 63226fe1a1d9c1d6d6e07d12dc26dbb31c9c9a7d Mon Sep 17 00:00:00 2001
From: Claude <noreply@anthropic.com>
Date: Wed, 5 Aug 2026 01:46:10 +0000
Subject: [PATCH] El seed de velas respeta el circuit breaker de ban (grupo
 spot)
MIME-Version: 1.0
Content-Type: text/plain; charset=UTF-8
Content-Transfer-Encoding: 8bit

_seed_klines() pedía REST cada 60s sin chequear si 'spot' ya estaba
baneado -- a diferencia de _loop_oi, que sí respeta el ban de
'futures'. Con un ban activo (típico justo después de un redeploy,
que reinicia el proceso y fuerza a re-sembrar los 7 intervalos),
esto insistía contra Binance en cada vuelta y podía extender el ban
en vez de dejarlo enfriar (mismo mecanismo ya documentado en
_refrescar_ban_estado_desde_disco). Ahora chequea _BAN_CHECK antes
de pedir, y registra el ban vía _BAN_REGISTRAR si lo detecta.
---
 ws_hub.py | 31 +++++++++++++++++++++++++++++++
 1 file changed, 31 insertions(+)

diff --git a/ws_hub.py b/ws_hub.py
index 111bb7c..a364e13 100644
--- a/ws_hub.py
+++ b/ws_hub.py
@@ -184,11 +184,32 @@ def _seed_klines():
     timeout en frío de Render, etc.), se reintenta suave cada 60s
     hasta completarlo -- mientras tanto get_klines devuelve None y el
     endpoint usa su fallback REST de siempre.
+
+    FIX: esta función NO respetaba el circuit breaker de ban (a
+    diferencia de _loop_oi, que sí chequea _BAN_CHECK). Con un ban
+    "spot" activo al arrancar el proceso (típico después de un
+    redeploy: Render reinicia, el hub arranca de cero y tiene que
+    re-sembrar los 7 intervalos), este loop igual salía a pedir REST
+    cada 60s SIN mirar si ya estábamos baneados -- y según el propio
+    comentario de _refrescar_ban_estado_desde_disco más abajo en este
+    archivo, insistir contra Binance durante un ban activo es
+    justamente lo que lo extiende (un ban de ~15min se había alargado
+    a ~2hs por esto mismo, en el poll de OI, antes de agregarle el
+    chequeo ahí). Acá le faltaba el mismo chequeo. Ahora: si "spot"
+    está baneado, no pide nada y espera a que se cumpla el ban antes
+    de reintentar.
     """
     while True:
         pendientes = [iv for iv in INTERVALOS if not _estado["klines_seed_ok"][iv]]
         if not pendientes:
             return
+
+        if _BAN_CHECK and _BAN_CHECK("spot"):
+            # No insistir mientras el ban siga vigente -- cada intento
+            # durante un ban puede extenderlo en vez de dejarlo enfriar.
+            time.sleep(15)
+            continue
+
         for iv in pendientes:
             datos, err = _get_rest(
                 SPOT_REST, "/api/v3/klines",
@@ -203,6 +224,16 @@ def _seed_klines():
             else:
                 with _lock:
                     _estado["ultimo_error"] = f"seed klines {iv}: {err}"
+                if isinstance(datos, dict) and _BAN_REGISTRAR:
+                    # Si esto fue un -1003, registrarlo en el circuit
+                    # breaker para que TODO el proxy (y este mismo loop,
+                    # en la próxima vuelta) lo respete de inmediato.
+                    _BAN_REGISTRAR("spot", datos)
+                if _BAN_CHECK and _BAN_CHECK("spot"):
+                    # Ya sabemos que estamos baneados -- cortar acá esta
+                    # vuelta en vez de seguir probando los intervalos
+                    # pendientes que quedan (todos van a fallar igual).
+                    break
         time.sleep(60)
 
 
-- 
2.43.0
