<div align="center">
  <img src="assets/yaruk-os-logo.png" alt="Yaruk-OS logo" width="220" />

  # Yaruk-OS

  _“Shedding light on complex documents.”_
</div>

**Yaruk-OS**, teknik dokümanları (devre şemaları, grafikler, tablolar, LaTeX/formüller, çok sütunlu akademik düzenler) **mekânsal yerleşimi bozmadan** Markdown/JSON’a dönüştürmek için tasarlanmış, **donanım ve işletim sistemi bağımsız** (plug & play) bir **PDF orkestratörü**dür.

Tek bir “her şeyi yapan” motor yerine; dokümanı **segmentler**, içeriği **sınıflandırır**, en uygun motorlara **yönlendirir** ve sonuçları **ortak bir ara temsil (Canonical IR)** üzerinde birleştirir.

## Neden?
- **Mekânsal bütünlük**: `bbox`, sayfa numarası, okuma sırası ve blok ilişkileri korunur.
- **Doğru araca doğru iş**: Formül, tablo, layout ve genel metin için farklı motorlar devreye girer.
- **Graceful degradation**: GPU/VRAM yoksa veya OOM olursa otomatik fallback.
- **İzolasyon**: Motorlar kütüphane çakışmalarını önlemek için izole worker süreçlerinde çalışır.

## Motorlar (hedeflenen)
- **MinerU**: Matematik/formül ve akademik düzen
- **OpenDataLoader-PDF**: Okuma sırası + koordinat/layout iskeleti
- **Docling**: Tablo/semantik yapı analizi
- **Marker**: Hızlı genel Markdown çıkarımı
- **MarkItDown**: Ofis belgeleri için hafif dönüşüm

## Mimari özet
1. **Giriş**: Sürükle-bırak + batch processing + dayanıklı kuyruk (SQLite/SQLModel).
2. **Karar**: Manuel mod (uzman) veya Auto mod (sayfa/blok bazlı hızlı ön analiz).
3. **Canonical IR**: Tüm çıktılar tek şemaya normalize edilir (Pydantic).
4. **Orkestrasyon**: Segmentasyon → dinamik yönlendirme → birleştirme.
5. **Fallback**: Hata/OOM/düşük güven durumunda alternatif sağlayıcı zinciri.
6. **Çıktı**: `.md` + `.json` + birleştirilmiş varyantlar (örn. `merged.md`).

## Yol haritası (fazlar)
- **Faz 1**: Proje iskeleti, Provider abstraction, Canonical IR, VRAM-korumalı görev kuyruğu
- **Faz 2**: İzole süreç mimarisiyle motor entegrasyonları
- **Faz 3**: Dinamik routing + fallback zinciri
- **Faz 4**: TUI (Textual) + GUI (PySide6) + manuel onay/inceleme akışı
- **Faz 5**: Flatpak & AppImage paketleme + regresyon testleri

## Durum
Bu depo şu an **tasarım ve planlama** aşamasında; uygulama iskeleti ve ilk çalışma sürümü kademeli olarak eklenecek.

## Lisans
Bkz. `LICENSE`.
