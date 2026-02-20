# -*- coding: utf-8 -*-
"""
Generiert einzigartige .ico-Dateien fuer jede Anwendung des DateiScanner-Projekts.
Einmalig ausfuehren: python generate_icons.py
"""

import os
import math
from PIL import Image, ImageDraw, ImageFont

ICONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons")
SIZES = [16, 32, 48, 64, 128]


def _get_font(size):
    """Versucht eine TrueType-Schrift zu laden, Fallback auf Default."""
    for name in ["segoeui.ttf", "arial.ttf", "calibri.ttf"]:
        try:
            return ImageFont.truetype(name, size)
        except (OSError, IOError):
            continue
    return ImageFont.load_default()


def _get_bold_font(size):
    """Versucht eine fette TrueType-Schrift zu laden."""
    for name in ["segoeuib.ttf", "arialbd.ttf", "calibrib.ttf"]:
        try:
            return ImageFont.truetype(name, size)
        except (OSError, IOError):
            continue
    return _get_font(size)


def _save_ico(images, name):
    """Speichert eine Liste von PIL Images als .ico mit mehreren Groessen."""
    path = os.path.join(ICONS_DIR, f"{name}.ico")
    # Groesstes Bild als Basis, kleinere als append_images
    largest = images[-1]  # 128x128
    smaller = images[:-1]  # 16, 32, 48, 64
    largest.save(path, format='ICO', append_images=smaller,
                 sizes=[(s, s) for s in SIZES])
    print(f"  -> {path} ({os.path.getsize(path):,} bytes)")


def _create_base(size, bg_color):
    """Erstellt ein Basis-Image mit abgerundetem Hintergrund."""
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    margin = max(1, size // 16)
    r = size // 4
    draw.rounded_rectangle(
        [margin, margin, size - margin - 1, size - margin - 1],
        radius=r, fill=bg_color
    )
    return img, draw


def create_gui_icon():
    """Haupt-GUI: Blaues Icon mit Datenbank-Symbol (Zylinder + Lupe)."""
    name = "gui_main"
    size = 128
    bg = (30, 100, 200, 255)
    img, draw = _create_base(size, bg)

    # Datenbank-Zylinder
    cx, cy = 52, 58
    rw, rh = 28, 10
    h = 40
    # Koerper
    draw.rectangle([cx - rw, cy, cx + rw, cy + h], fill=(220, 235, 255, 255))
    # Untere Ellipse
    draw.ellipse([cx - rw, cy + h - rh, cx + rw, cy + h + rh], fill=(180, 210, 255, 255))
    # Mittlere Linie
    draw.ellipse([cx - rw, cy + h // 2 - rh, cx + rw, cy + h // 2 + rh],
                 fill=(200, 225, 255, 255), outline=(30, 100, 200, 180), width=1)
    # Obere Ellipse
    draw.ellipse([cx - rw, cy - rh, cx + rw, cy + rh], fill=(240, 248, 255, 255),
                 outline=(20, 70, 160, 255), width=2)

    # Lupe rechts unten
    lx, ly = 88, 88
    lr = 16
    draw.ellipse([lx - lr, ly - lr, lx + lr, ly + lr],
                 outline=(255, 255, 255, 255), width=4)
    draw.line([lx + lr - 4, ly + lr - 4, lx + lr + 10, ly + lr + 10],
              fill=(255, 255, 255, 255), width=4)

    # Multi-Size erzeugen
    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_search_icon():
    """Erweiterte Dateisuche: Gruenes Icon mit grosser Lupe."""
    name = "search"
    size = 128
    bg = (34, 139, 34, 255)  # Forest Green
    img, draw = _create_base(size, bg)

    # Grosse Lupe
    cx, cy = 54, 52
    r = 26
    draw.ellipse([cx - r, cy - r, cx + r, cy + r],
                 outline=(255, 255, 255, 255), width=5)
    # Innerer Bereich leicht heller
    draw.ellipse([cx - r + 6, cy - r + 6, cx + r - 6, cy + r - 6],
                 fill=(50, 170, 50, 200))
    # Griff
    draw.line([cx + r - 6, cy + r - 6, cx + r + 24, cy + r + 24],
              fill=(255, 255, 255, 255), width=6)

    # "+" im Glas fuer "erweitert"
    font = _get_bold_font(30)
    draw.text((cx, cy), "+", fill=(255, 255, 255, 230), anchor="mm", font=font)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_simple_search_icon():
    """Einfache Dateisuche: Blaues Icon mit Lupe."""
    name = "search_simple"
    size = 128
    bg = (65, 105, 225, 255)  # Royal Blue
    img, draw = _create_base(size, bg)

    # Lupe
    cx, cy = 54, 52
    r = 26
    draw.ellipse([cx - r, cy - r, cx + r, cy + r],
                 outline=(255, 255, 255, 255), width=5)
    draw.ellipse([cx - r + 6, cy - r + 6, cx + r - 6, cy + r - 6],
                 fill=(90, 130, 240, 200))
    draw.line([cx + r - 6, cy + r - 6, cx + r + 24, cy + r + 24],
              fill=(255, 255, 255, 255), width=6)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_duplicate_folder_icon():
    """Duplikat-Ordner-Manager: Orange mit ueberlappenden Ordnern."""
    name = "duplicate_folders"
    size = 128
    bg = (230, 126, 34, 255)  # Orange
    img, draw = _create_base(size, bg)

    def draw_folder(x, y, w, h, color, outline_color):
        # Tab oben
        tab_w = w // 3
        tab_h = h // 6
        draw.rectangle([x, y, x + tab_w, y + tab_h], fill=color)
        # Ordner-Koerper
        draw.rounded_rectangle([x, y + tab_h, x + w, y + h], radius=4,
                               fill=color, outline=outline_color, width=2)

    # Hinterer Ordner
    draw_folder(40, 30, 56, 44, (255, 200, 120, 200), (180, 100, 20, 255))
    # Vorderer Ordner
    draw_folder(32, 50, 56, 44, (255, 220, 150, 240), (200, 120, 30, 255))

    # "=" Zeichen (Gleichheit / Duplikat)
    font = _get_bold_font(28)
    draw.text((100, 80), "=", fill=(255, 255, 255, 230), anchor="mm", font=font)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_advanced_duplicate_icon():
    """Advanced Duplicate Manager: Rot mit ueberlappenden Dateien."""
    name = "duplicate_advanced"
    size = 128
    bg = (192, 57, 43, 255)  # Dunkelrot
    img, draw = _create_base(size, bg)

    def draw_file(x, y, w, h, color, outline):
        ear = w // 4
        # Eselsohr-Polygon
        points = [(x, y), (x + w - ear, y), (x + w, y + ear), (x + w, y + h),
                  (x, y + h)]
        draw.polygon(points, fill=color, outline=outline, width=2)
        # Knick-Linie
        draw.line([(x + w - ear, y), (x + w - ear, y + ear), (x + w, y + ear)],
                  fill=outline, width=1)

    # Hintere Datei
    draw_file(46, 26, 48, 60, (255, 180, 170, 200), (140, 40, 30, 255))
    # Vordere Datei
    draw_file(34, 42, 48, 60, (255, 200, 190, 240), (160, 50, 40, 255))

    # Blitz (schnell)
    font = _get_bold_font(32)
    draw.text((100, 84), "2x", fill=(255, 255, 255, 230), anchor="mm", font=font)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_fast_duplicate_icon():
    """Fast Duplicate Finder: Gelb mit Blitz."""
    name = "duplicate_fast"
    size = 128
    bg = (241, 196, 15, 255)  # Gelb/Gold
    img, draw = _create_base(size, bg)

    # Blitz-Symbol
    bolt_points = [
        (68, 20), (42, 66), (60, 66),
        (48, 110), (90, 54), (70, 54),
        (82, 20)
    ]
    draw.polygon(bolt_points, fill=(255, 255, 255, 240), outline=(180, 140, 0, 255), width=2)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_fast_folder_duplicate_icon():
    """Fast Folder Duplicate Finder: Gold mit Blitz + Ordner."""
    name = "duplicate_fast_folder"
    size = 128
    bg = (211, 166, 0, 255)  # Dunkles Gold
    img, draw = _create_base(size, bg)

    # Ordner
    draw.rectangle([24, 38, 52, 48], fill=(255, 230, 130, 230))
    draw.rounded_rectangle([24, 48, 80, 90], radius=3,
                           fill=(255, 230, 130, 230), outline=(160, 120, 0, 255), width=2)

    # Blitz
    bolt = [(90, 28), (72, 60), (84, 60), (74, 100), (104, 48), (90, 48), (98, 28)]
    draw.polygon(bolt, fill=(255, 255, 255, 240), outline=(160, 120, 0, 255), width=2)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_disk_usage_icon():
    """Speicherverbrauch: Lila mit Kreisdiagramm."""
    name = "disk_usage"
    size = 128
    bg = (142, 68, 173, 255)  # Lila
    img, draw = _create_base(size, bg)

    cx, cy = 64, 64
    r = 36
    # Voller Kreis (Hintergrund)
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(200, 160, 220, 255))
    # Tortenstueck (75% voll)
    draw.pieslice([cx - r, cy - r, cx + r, cy + r], start=-90, end=180,
                  fill=(255, 255, 255, 240))
    # Kleiner innerer Kreis (Donut-Effekt)
    ir = 14
    draw.ellipse([cx - ir, cy - ir, cx + ir, cy + ir], fill=bg)

    # Prozent-Text
    font = _get_bold_font(16)
    draw.text((cx, cy), "%", fill=(255, 255, 255, 230), anchor="mm", font=font)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_music_icon():
    """Musik-Manager: Pink mit Musiknote."""
    name = "music"
    size = 128
    bg = (219, 68, 134, 255)  # Pink
    img, draw = _create_base(size, bg)

    # Musiknote - Hals
    draw.rectangle([72, 28, 78, 88], fill=(255, 255, 255, 240))
    # Notenkopf unten
    draw.ellipse([48, 76, 78, 100], fill=(255, 255, 255, 240))
    # Fahne oben
    draw.arc([78, 28, 104, 62], start=-90, end=90,
             fill=(255, 255, 255, 240), width=5)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_systray_icon():
    """System Tray: Dunkelblaues Icon mit Zahnrad."""
    name = "systray"
    size = 128
    bg = (44, 62, 80, 255)  # Dunkelblau/Grau
    img, draw = _create_base(size, bg)

    cx, cy = 64, 64
    outer_r = 34
    inner_r = 20
    teeth = 8
    tooth_w = 12

    # Zahnrad zeichnen
    for i in range(teeth):
        angle = math.radians(i * 360 / teeth)
        tx = cx + outer_r * math.cos(angle)
        ty = cy + outer_r * math.sin(angle)
        half = tooth_w // 2
        draw.ellipse([tx - half, ty - half, tx + half, ty + half],
                     fill=(200, 210, 220, 240))

    # Innerer Ring
    draw.ellipse([cx - inner_r - 6, cy - inner_r - 6, cx + inner_r + 6, cy + inner_r + 6],
                 fill=(200, 210, 220, 240))
    # Loch in der Mitte
    draw.ellipse([cx - inner_r + 4, cy - inner_r + 4, cx + inner_r - 4, cy + inner_r - 4],
                 fill=bg)

    # "S" fuer Scanner
    font = _get_bold_font(20)
    draw.text((cx, cy), "S", fill=(200, 210, 220, 240), anchor="mm", font=font)

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def create_integrity_icon():
    """Integritaetspruefung: Tuerkis mit Schild + Haekchen."""
    name = "integrity"
    size = 128
    bg = (22, 160, 133, 255)  # Tuerkis
    img, draw = _create_base(size, bg)

    # Schild-Form
    shield = [
        (64, 22), (96, 38), (96, 72), (64, 106), (32, 72), (32, 38)
    ]
    draw.polygon(shield, fill=(255, 255, 255, 230), outline=(15, 120, 100, 255), width=3)

    # Haekchen
    check = [(44, 64), (58, 82), (84, 48)]
    draw.line(check, fill=(22, 160, 133, 255), width=6, joint="curve")

    frames = [img.resize((s, s), Image.LANCZOS) for s in SIZES]
    _save_ico(frames, name)


def main():
    os.makedirs(ICONS_DIR, exist_ok=True)
    print(f"Generiere Icons in: {ICONS_DIR}\n")

    create_gui_icon()
    create_search_icon()
    create_simple_search_icon()
    create_duplicate_folder_icon()
    create_advanced_duplicate_icon()
    create_fast_duplicate_icon()
    create_fast_folder_duplicate_icon()
    create_disk_usage_icon()
    create_music_icon()
    create_systray_icon()
    create_integrity_icon()

    print(f"\nFertig! {len(os.listdir(ICONS_DIR))} Icons generiert.")


if __name__ == "__main__":
    main()
