"""
Procesador de Recibos — Streamlit Cloud App v3.0
Combina PASO_1 (extraer + clasificar) y PASO_2 (renombrar con CSV)
"""
from __future__ import annotations

import base64
import csv
import io
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import fitz  # PyMuPDF
import streamlit as st
import urllib.request

# ─── Configuracion ───────────────────────────────────────────────
MAC_JUNK = {".DS_Store"}
SIZE_THRESHOLD = 15 * 1024  # 15 KB
RFC_ALNUM_RE = re.compile(r"^[A-Z0-9]{12,13}$")

DEFAULT_CSV_URL = (
    "https://redash.humand.co/api/queries/28431/results.csv"
    "?api_key=2TcfZFwFRwpSxupg7vV3US5KZonBdgIlMLmvcDSX"
)

# Humand logo — loaded from repo file at startup
LOGO_PATH = Path(__file__).parent / "humand_logo.svg"


# ─── Funciones auxiliares ─────────────────────────────────────────
def is_junk(p: Path) -> bool:
    return p.name in MAC_JUNK or p.name.startswith("._")


def safe_mkdir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def clean_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\n]', "", name).strip()


def nombre_unico(path_destino: Path) -> Path:
    if not path_destino.exists():
        return path_destino
    base = path_destino.stem
    ext = path_destino.suffix
    parent = path_destino.parent
    i = 1
    while True:
        cand = parent / f"{base}_{i}{ext}"
        if not cand.exists():
            return cand
        i += 1


def extract_rfc(pdf_path: Path, x: float, y: float, width=150, height=30) -> str | None:
    try:
        with fitz.open(str(pdf_path)) as pdf:
            page = pdf[0]
            rect = fitz.Rect(x, y, x + width, y + height)
            text = page.get_text("text", clip=rect).strip()
            cleaned = re.sub(r"\s+", "", text)
            m = re.search(r"([A-Z]{3,4}\d{6}[A-Z0-9]{2,3})", cleaned)
            return m.group(1) if m else None
    except Exception:
        return None


# ─── Extraccion de archivos ───────────────────────────────────────
def extract_rar(rar_path: Path, dest_dir: Path, log=None) -> bool:
    """Extrae RAR usando multiples metodos. Prioriza unar (igual que en Mac)."""
    tools = [
        ("unar", ["unar", "-force-overwrite", "-no-directory", "-output-directory", str(dest_dir), str(rar_path)]),
        ("7z", ["7z", "x", "-y", f"-o{dest_dir}", str(rar_path)]),
        ("unrar", ["unrar", "x", "-o+", str(rar_path), str(dest_dir) + "/"]),
        ("unrar-free", ["unrar-free", "-x", str(rar_path), str(dest_dir) + "/"]),
    ]
    errors = []
    for name, cmd in tools:
        try:
            result = subprocess.run(cmd, capture_output=True, timeout=120)
            if result.returncode == 0:
                if log:
                    log(f"    (extraido con {name})")
                return True
            else:
                stderr = result.stderr.decode("utf-8", errors="replace")[:100]
                errors.append(f"{name}: exit {result.returncode} - {stderr}")
        except FileNotFoundError:
            errors.append(f"{name}: no instalado")
        except subprocess.TimeoutExpired:
            errors.append(f"{name}: timeout")

    # Fallback: rarfile de Python
    try:
        import rarfile
        rarfile.UNRAR_TOOL = "unrar"
        with rarfile.RarFile(str(rar_path), "r") as rf:
            rf.extractall(str(dest_dir))
        if log:
            log(f"    (extraido con rarfile python)")
        return True
    except Exception as e:
        errors.append(f"rarfile: {e}")

    if log:
        for err in errors:
            log(f"    [DEBUG] {err}")
    return False


def extract_archives(work_dir: Path, log):
    """Extrae recursivamente ZIP/RAR dentro de work_dir."""
    extracted_any = True
    total = 0

    while extracted_any:
        extracted_any = False
        for p in list(work_dir.rglob("*")):
            if not p.is_file() or is_junk(p):
                continue
            suf = p.suffix.lower()

            if suf == ".zip":
                try:
                    with zipfile.ZipFile(p, "r") as z:
                        z.extractall(p.parent)
                    p.unlink(missing_ok=True)
                    extracted_any = True
                    total += 1
                    log(f"[OK] Extraido ZIP: {p.name}")
                except zipfile.BadZipFile:
                    log(f"[WARN] ZIP invalido: {p.name}")

            elif suf == ".rar":
                if extract_rar(p, p.parent, log=log):
                    p.unlink(missing_ok=True)
                    extracted_any = True
                    total += 1
                    log(f"[OK] Extraido RAR: {p.name}")
                else:
                    log(f"[WARN] RAR error: {p.name} (no se pudo extraer)")

    return total


# ─── Recoleccion y clasificacion ──────────────────────────────────
def collect_pdfs_and_delete_xml(from_dir: Path) -> list[Path]:
    pdfs = []
    for p in from_dir.rglob("*"):
        if not p.is_file() or is_junk(p):
            continue
        suf = p.suffix.lower()
        if suf == ".xml":
            p.unlink(missing_ok=True)
        elif suf == ".pdf":
            pdfs.append(p)
    return pdfs


def detectar_periodo_desde_ruta(pdf_path: Path) -> str:
    partes = list(pdf_path.parts) + [pdf_path.stem]
    for parte in partes:
        parte_up = parte.upper()
        m = re.search(r'(?:QNA|QNAL)[_\s]*(\d+)', parte_up)
        if m:
            return f"Q{m.group(1)}"
        m = re.search(r'(?:SEMANA|SEM)[_\s]*(\d+)', parte_up)
        if m:
            return f"SEM{m.group(1)}"
    return "GENERAL"


def classify_pdfs_by_period(pdfs: list[Path], output_dir: Path, log) -> dict:
    stats = {"aro": 0, "zentrix": 0, "periodos": set()}

    for src in pdfs:
        periodo = detectar_periodo_desde_ruta(src)

        if src.stat().st_size > SIZE_THRESHOLD:
            tag = "ZENTRIX"
            stats["zentrix"] += 1
        else:
            tag = "ARO"
            stats["aro"] += 1

        dest_dir = output_dir / periodo / tag
        safe_mkdir(dest_dir)
        stats["periodos"].add(periodo)

        dest = nombre_unico(dest_dir / src.name)
        shutil.copy2(src, dest)

    stats["periodos_set"] = stats["periodos"].copy()
    stats["periodos"] = len(stats["periodos"])
    return stats


# ─── Renombrado por RFC ───────────────────────────────────────────
def rename_pdfs_by_rfc(pdf_dir: Path, x: float, y: float, log) -> int:
    renamed = 0
    for p in sorted(pdf_dir.glob("*.pdf")):
        rfc = extract_rfc(p, x, y)
        if not rfc:
            continue
        rfc_clean = clean_filename(rfc)
        new_path = nombre_unico(pdf_dir / f"{rfc_clean}.pdf")
        p.rename(new_path)
        renamed += 1
    return renamed


# ─── Funciones de PASO_2 ─────────────────────────────────────────
def descargar_csv(url: str) -> dict:
    with urllib.request.urlopen(url) as response:
        text = response.read().decode("utf-8")
    csv_file = io.StringIO(text)
    reader = csv.reader(csv_file)
    headers = next(reader, None)
    if not headers:
        return {}

    try:
        col_user = headers.index("username")
    except ValueError:
        col_user = 0
    try:
        col_rfc = headers.index("RFC")
    except ValueError:
        col_rfc = 1

    mapa = {}
    for row in reader:
        if len(row) <= max(col_user, col_rfc):
            continue
        user = (row[col_user] or "").strip()
        rfc = (row[col_rfc] or "").strip()
        if not rfc or rfc.lower() == "null" or not user:
            continue
        mapa[rfc.upper()] = user

    return mapa


def normalizar_periodo(nombre_carpeta: str) -> str:
    s = (nombre_carpeta or "").strip().upper()
    s = re.sub(r"\s+", " ", s)
    mnum = re.search(r"(\d+)", s)
    num = mnum.group(1) if mnum else None

    if ("SEMANA" in s) or re.match(r"^SEM\b", s):
        return f"SEM{num}" if num else "SEM"
    if s.startswith("QNA") or s.startswith("Q"):
        return f"Q{num}" if num else "Q"
    return s.replace(" ", "")


def extraer_base_y_sufijo(nombre_sin_extension: str):
    s = nombre_sin_extension.strip()
    if "_" in s:
        base, suf = s.rsplit("_", 1)
        if suf.isdigit():
            return base, int(suf)
    return s, None


def renombrar_con_csv(root_dir: Path, mapa: dict, log) -> dict:
    users_set = set(mapa.values())
    stats = {"renombrados": 0, "sin_renombrar": 0, "ignorados": 0, "total": 0}

    for current_root, dirs, files in os.walk(root_dir):
        dirs[:] = [d for d in dirs if "SINRENOMBRAR" not in d.upper()]
        current = Path(current_root)

        actual = current.name.upper()
        if actual in ("ARO", "ZENTRIX", "ZTX"):
            sem_raw = current.parent.name
            tag = "ARO" if actual == "ARO" else "ZTX"
        else:
            sem_raw = current.name
            tag = "ZTX"
        sem = normalizar_periodo(sem_raw)

        sin_ren_name = f"{tag}_{sem}_SINRENOMBRAR"
        sin_ren_dir = root_dir / sin_ren_name

        for fname in files:
            fpath = current / fname
            name = fpath.stem
            ext = fpath.suffix
            if ext.lower() != ".pdf":
                continue

            stats["total"] += 1

            if name.startswith("Recibo_"):
                stats["ignorados"] += 1
                continue

            base, suf = extraer_base_y_sufijo(name)
            base_up = base.upper()
            es_rfc = bool(RFC_ALNUM_RE.match(base_up))

            if es_rfc:
                if base_up in mapa:
                    user = mapa[base_up]
                    new_base = f"Recibo_{sem}_{tag}_{user}"
                    if suf is not None:
                        new_base += f"_{suf}"
                    new_path = nombre_unico(current / f"{new_base}{ext}")
                    os.rename(fpath, new_path)
                    stats["renombrados"] += 1
                    log(f"[RENOMBRAR] {fname} -> {new_path.name}")
                else:
                    safe_mkdir(sin_ren_dir)
                    dest = nombre_unico(sin_ren_dir / fname)
                    os.rename(fpath, dest)
                    stats["sin_renombrar"] += 1
                    log(f"[SIN RFC] {fname} -> {sin_ren_name}/")
                continue

            if base in users_set:
                new_base = f"Recibo_{sem}_{tag}_{base}"
                if suf is not None:
                    new_base += f"_{suf}"
                new_path = nombre_unico(current / f"{new_base}{ext}")
                os.rename(fpath, new_path)
                stats["renombrados"] += 1
                log(f"[RENOMBRAR] {fname} -> {new_path.name}")
                continue

            stats["ignorados"] += 1

    return stats


# ─── Proceso completo ─────────────────────────────────────────────
def procesar_todo(uploaded_files, csv_url, progress_bar, log):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        input_dir = tmp / "input"
        output_dir = tmp / "output"
        safe_mkdir(input_dir)
        safe_mkdir(output_dir)

        # 1. Guardar archivos subidos
        log("[INFO] Guardando archivos subidos...")
        for f in uploaded_files:
            dest = input_dir / f.name
            dest.write_bytes(f.getbuffer())
        progress_bar.progress(10, text="Archivos guardados")

        # 2. Extraer recursivamente (ZIP + RAR)
        log("[INFO] Extrayendo archivos comprimidos (recursivo)...")
        total_extracted = extract_archives(input_dir, log)
        log(f"[OK] {total_extracted} archivos comprimidos extraidos")
        progress_bar.progress(25, text="Archivos extraidos")

        # 3. Recoger PDFs
        log("[INFO] Recolectando PDFs...")
        pdfs = collect_pdfs_and_delete_xml(input_dir)
        log(f"[OK] {len(pdfs)} PDFs encontrados")
        progress_bar.progress(35, text=f"{len(pdfs)} PDFs encontrados")

        if not pdfs:
            log("[WARN] No se encontraron PDFs para procesar")
            return None

        # 4. Clasificar en periodos + ARO / ZENTRIX
        log("[INFO] Clasificando PDFs por periodo y tamano...")
        class_stats = classify_pdfs_by_period(pdfs, output_dir, log)
        log(f"[OK] ARO: {class_stats['aro']} | ZENTRIX: {class_stats['zentrix']} | Periodos: {class_stats['periodos']}")
        progress_bar.progress(50, text="PDFs clasificados")

        # 5. Renombrar por RFC en cada subcarpeta
        log("[INFO] Extrayendo RFCs de los PDFs...")
        total_rfc = 0
        for aro_dir in output_dir.rglob("ARO"):
            if aro_dir.is_dir():
                total_rfc += rename_pdfs_by_rfc(aro_dir, x=61, y=180, log=log)
        for ztx_dir in output_dir.rglob("ZENTRIX"):
            if ztx_dir.is_dir():
                total_rfc += rename_pdfs_by_rfc(ztx_dir, x=54, y=142, log=log)
        log(f"[OK] {total_rfc} PDFs renombrados por RFC")
        progress_bar.progress(65, text="RFCs extraidos")

        # 6. Descargar CSV y renombrar
        log("[INFO] Descargando CSV desde Redash...")
        try:
            mapa = descargar_csv(csv_url)
            log(f"[OK] {len(mapa)} RFCs cargados desde CSV")
        except Exception as e:
            log(f"[ERROR] No se pudo descargar el CSV: {e}")
            mapa = {}

        progress_bar.progress(75, text="CSV descargado")

        if mapa:
            log("[INFO] Renombrando con nomenclatura final...")
            rename_stats = renombrar_con_csv(output_dir, mapa, log)
        else:
            rename_stats = {"renombrados": 0, "sin_renombrar": 0, "ignorados": 0,
                           "total": class_stats["aro"] + class_stats["zentrix"]}

        progress_bar.progress(85, text="Generando ZIPs de salida...")

        # 7. Crear ZIPs por periodo+categoria, archivos PLANOS (sin carpetas)
        # Guardar en /tmp (persiste entre reruns de Streamlit)
        persist_dir = Path("/tmp/recibos_resultado")
        if persist_dir.exists():
            shutil.rmtree(persist_dir)
        safe_mkdir(persist_dir)

        zips_info = {}
        all_pdfs = list(output_dir.rglob("*.pdf"))

        # Agrupar PDFs por periodo y tipo
        groups = {}  # key: (periodo, tipo) -> list of pdf paths
        for file in all_pdfs:
            rel = str(file.relative_to(output_dir))
            rel_upper = rel.upper()

            # Detectar periodo (primer componente del path)
            parts = file.relative_to(output_dir).parts
            periodo = parts[0] if len(parts) > 1 else "GENERAL"

            # Detectar tipo
            is_sinrenombrar = "SINRENOMBRAR" in rel_upper
            if is_sinrenombrar:
                if "ZTX" in rel_upper or "ZENTRIX" in rel_upper:
                    tipo = "ZENTRIX_SINRENOMBRAR"
                elif "ARO" in rel_upper:
                    tipo = "ARO_SINRENOMBRAR"
                else:
                    tipo = "OTROS_SINRENOMBRAR"
            elif "ZENTRIX" in rel_upper:
                tipo = "ZENTRIX"
            elif "/ARO/" in ("/" + rel_upper + "/") or rel_upper.startswith("ARO/"):
                tipo = "ARO"
            else:
                tipo = "OTROS"

            key = (periodo, tipo)
            if key not in groups:
                groups[key] = []
            groups[key].append(file)

        # Crear un ZIP por cada grupo (periodo + tipo), archivos planos
        for (periodo, tipo), files in sorted(groups.items()):
            zip_name = f"{tipo}_{periodo}"
            zip_path = persist_dir / f"{zip_name}.zip"
            count = 0
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for file in files:
                    # Archivo plano: solo el nombre, sin subcarpetas
                    zf.write(file, file.name)
                    count += 1

            if count > 0:
                size_mb = zip_path.stat().st_size / (1024 * 1024)
                zips_info[zip_name] = {
                    "path": str(zip_path),
                    "count": count,
                    "size_mb": size_mb,
                    "periodo": periodo,
                    "tipo": tipo,
                }
                log(f"[OK] {zip_name}: {count} archivos ({size_mb:.1f} MB)")
            else:
                zip_path.unlink(missing_ok=True)

        log("[DONE] Proceso completado")
        progress_bar.progress(100, text="Listo!")

        final_stats = {
            "pdfs_encontrados": class_stats["aro"] + class_stats["zentrix"],
            "aro": class_stats["aro"],
            "zentrix": class_stats["zentrix"],
            **rename_stats,
        }

        return {"zips_info": zips_info, "stats": final_stats}


# ─── Estilos CSS ─────────────────────────────────────────────────
def inject_custom_css():
    st.markdown("""
    <style>
    /* ── Tipografia Roboto ── */
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'Roboto', sans-serif !important;
    }

    /* ── Fondo blanco limpio ── */
    .stApp {
        background-color: #ffffff !important;
    }
    section[data-testid="stSidebar"] {
        background-color: #f8f9fc !important;
        border-right: 1px solid #e8e9f0;
    }

    /* ── Header con logo ── */
    .humand-header {
        display: flex;
        align-items: center;
        gap: 16px;
        padding: 24px 0 16px 0;
        border-bottom: 2px solid #eff2ff;
        margin-bottom: 32px;
    }
    .humand-header img {
        height: 44px;
        border-radius: 8px;
    }
    .humand-header .title {
        font-size: 24px;
        font-weight: 600;
        color: #213478;
        letter-spacing: 0.2px;
        line-height: 1.3;
    }
    .humand-header .subtitle {
        font-size: 14px;
        color: #636271;
        letter-spacing: 0.2px;
        margin-top: 2px;
    }

    /* ── Cards de estadisticas ── */
    [data-testid="stMetric"] {
        background: #f8f9fc;
        border: 1px solid #e8e9f0;
        border-radius: 8px;
        padding: 16px 20px;
    }
    [data-testid="stMetricLabel"] {
        font-size: 12px !important;
        color: #636271 !important;
        font-weight: 400 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    [data-testid="stMetricValue"] {
        font-size: 32px !important;
        font-weight: 600 !important;
        color: #213478 !important;
    }

    /* ── Botones de descarga ── */
    .stDownloadButton > button {
        background-color: #213478 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 12px 24px !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        letter-spacing: 0.2px;
        transition: background-color 0.2s ease;
    }
    .stDownloadButton > button:hover {
        background-color: #6f93eb !important;
        color: white !important;
    }

    /* ── Boton primario (Procesar) ── */
    .stButton > button[kind="primary"],
    button[data-testid="stBaseButton-primary"] {
        background-color: #213478 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        font-size: 16px !important;
        padding: 12px 24px !important;
        transition: background-color 0.2s ease;
    }
    .stButton > button[kind="primary"]:hover,
    button[data-testid="stBaseButton-primary"]:hover {
        background-color: #6f93eb !important;
    }

    /* ── Boton secundario ── */
    .stButton > button[kind="secondary"],
    button[data-testid="stBaseButton-secondary"] {
        background-color: #ffffff !important;
        color: #213478 !important;
        border: 1px solid #213478 !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
    }

    /* ── File uploader ── */
    [data-testid="stFileUploader"] {
        border: 2px dashed #c5cee8 !important;
        border-radius: 12px !important;
        padding: 24px !important;
        background: #fafbff !important;
    }

    /* ── Progress bar ── */
    .stProgress > div > div > div {
        background-color: #6f93eb !important;
    }

    /* ── Success box ── */
    .stSuccess {
        background-color: #f0faf0 !important;
        border-left-color: #34a853 !important;
        border-radius: 8px !important;
    }

    /* ── Ocultar hamburger menu y footer ── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* ── Section divider ── */
    .section-divider {
        height: 1px;
        background: #e8e9f0;
        margin: 24px 0;
    }

    /* ── Download section title ── */
    .download-title {
        font-size: 18px;
        font-weight: 600;
        color: #303036;
        margin-bottom: 8px;
        letter-spacing: 0.2px;
    }
    .download-subtitle {
        font-size: 14px;
        color: #636271;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)


# ─── Interfaz Streamlit ──────────────────────────────────────────

def mostrar_resultados():
    """Muestra estadisticas y botones de descarga desde session_state."""
    stats = st.session_state.get("resultado_stats")
    zips_info = st.session_state.get("resultado_zips_info")

    if not stats or not zips_info:
        return

    st.success("Proceso completado")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("PDFs encontrados", stats.get("pdfs_encontrados", 0))
    with col2:
        st.metric("ARO", stats.get("aro", 0))
    with col3:
        st.metric("ZENTRIX", stats.get("zentrix", 0))
    with col4:
        st.metric("Renombrados", stats.get("renombrados", 0))

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="download-title">Descargas</div>', unsafe_allow_html=True)
    st.markdown('<div class="download-subtitle">Cada boton descarga de forma independiente. Podes bajar todos.</div>', unsafe_allow_html=True)

    # Separar renombrados de sin renombrar
    renombrados = {k: v for k, v in zips_info.items() if "SINRENOMBRAR" not in k}
    sin_renombrar = {k: v for k, v in zips_info.items() if "SINRENOMBRAR" in k}

    # Mostrar renombrados
    if renombrados:
        cols = st.columns(min(len(renombrados), 3))
        for i, (zip_name, z) in enumerate(sorted(renombrados.items())):
            zip_path = Path(z["path"])
            with cols[i % len(cols)]:
                if zip_path.exists():
                    zip_bytes = zip_path.read_bytes()
                    tipo = z.get("tipo", "")
                    periodo = z.get("periodo", "")
                    st.download_button(
                        label=f"{tipo}  {periodo}  ({z['count']} archivos)",
                        data=zip_bytes,
                        file_name=f"{zip_name}.zip",
                        mime="application/zip",
                        use_container_width=True,
                        key=f"dl_{zip_name}",
                    )
                else:
                    st.warning(f"{zip_name}: sesion expirada")

    # Mostrar sin renombrar
    if sin_renombrar:
        st.markdown("")
        st.caption("Sin renombrar")
        cols2 = st.columns(min(len(sin_renombrar), 3))
        for i, (zip_name, z) in enumerate(sorted(sin_renombrar.items())):
            zip_path = Path(z["path"])
            with cols2[i % len(cols2)]:
                if zip_path.exists():
                    zip_bytes = zip_path.read_bytes()
                    tipo_base = z.get("tipo", "").replace("_SINRENOMBRAR", "")
                    periodo = z.get("periodo", "")
                    st.download_button(
                        label=f"{tipo_base}  {periodo}  ({z['count']} archivos)",
                        data=zip_bytes,
                        file_name=f"{zip_name}.zip",
                        mime="application/zip",
                        use_container_width=True,
                        key=f"dl_{zip_name}",
                    )
                else:
                    st.warning(f"{zip_name}: sesion expirada")

    if not zips_info:
        st.warning("No se generaron archivos para descargar.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    if st.button("Procesar nuevos archivos", use_container_width=True):
        st.session_state.pop("resultado_stats", None)
        st.session_state.pop("resultado_zips_info", None)
        st.session_state.pop("resultado_logs", None)
        st.rerun()


def main():
    st.set_page_config(
        page_title="Recibos | Humand",
        page_icon="📄",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    inject_custom_css()

    # ── Header con logo ──
    logo_b64 = ""
    if LOGO_PATH.exists():
        logo_b64 = base64.b64encode(LOGO_PATH.read_bytes()).decode()

    if logo_b64:
        st.markdown(f"""
        <div class="humand-header">
            <img src="data:image/svg+xml;base64,{logo_b64}" alt="Humand">
            <div>
                <div class="title">Procesador de Recibos</div>
                <div class="subtitle">Extrae, clasifica y renombra PDFs automaticamente</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="humand-header">
            <div>
                <div class="title">Procesador de Recibos</div>
                <div class="subtitle">Extrae, clasifica y renombra PDFs automaticamente</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Si ya hay resultados en session_state, mostrarlos directamente
    if "resultado_zips_info" in st.session_state:
        mostrar_resultados()
        return

    # ── Upload ──
    uploaded_files = st.file_uploader(
        "Arrastra archivos o hace click para seleccionar",
        type=["zip", "rar", "pdf"],
        accept_multiple_files=True,
        help="ZIP, RAR o PDF — podes subir varios a la vez",
    )

    if uploaded_files:
        total_size = sum(f.size for f in uploaded_files) / (1024 * 1024)
        st.caption(f"{len(uploaded_files)} archivo(s) seleccionado(s)  —  {total_size:.1f} MB en total")

    st.markdown("")

    csv_url = DEFAULT_CSV_URL

    if st.button("Procesar archivos", type="primary", disabled=not uploaded_files,
                  use_container_width=True):

        progress_bar = st.progress(0, text="Iniciando...")
        logs = []

        def log(msg):
            logs.append(msg)

        try:
            result = procesar_todo(
                uploaded_files, csv_url, progress_bar, log
            )
        except Exception as e:
            st.error(f"Error: {e}")
            with st.expander("Ver log de errores"):
                st.code("\n".join(logs), language="bash")
            result = None

        if result and result.get("zips_info"):
            # Guardar en session_state y rerun para mostrar botones persistentes
            st.session_state["resultado_stats"] = result["stats"]
            st.session_state["resultado_zips_info"] = result["zips_info"]
            st.session_state["resultado_logs"] = logs
            st.rerun()
        elif result is None:
            st.warning("No se encontraron PDFs para procesar.")
        else:
            st.warning("No se generaron archivos para descargar.")


if __name__ == "__main__":
    main()
