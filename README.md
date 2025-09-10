Sniffer Bot Light: 

Sniffer/auto-buy bot dla Solany:
- MOCK sniffer (TTL + debounce) do szybkich testów bez sieci
- REAL sniffer: WSS (PubSub) z watchdogiem + failover do HTTP pollera (getSignaturesForAddress + getTransaction)
- Heurystyki pump.fun + dogrywanie metadanych z RPC
- BuyEngine z trybem one-slot (Sniffing → PassiveToken → Sell → Sniffing)
- RpcBroadcaster (trait) + produkcyjny RpcManager
- Prosty GUI (eframe/egui) — przyciski i skróty S/Q/W

## OCZEKIWANE PARAMETRY: 

Latencja detekcji od zdarzenia do kandydata: 30–150 ms (zależnie od feedu i hostingu).
Throughput: dziesiątki–setki kandydatów/min (filtr redukuje do < 10/min istotnych).
Bufor:
Rozmiar: 512–2048 pozycji.
TTL: 15–60 s (konfigurowalne).
Buy path:
Budowa 8–16 wariantów TX (nonce/prio): 5–25 ms łącznie.
Broadcast do 3–6 RPC równolegle; pierwszy sukces: typowo 150–700 ms.
Timeout per send: 4–8 s; retries: 1–3; skip_preflight: true.
Skuteczność “first success”: > 90% przy poprawnych endpointach i adequate fee.
SELL:
Manual trigger natychmiastowy; TX build ~5–15 ms; broadcast jak wyżej.
GUI:
Odświeżanie: 100–250 ms.
Zużycie zasobów (szacunek, jedna instancja):
CPU: 1–2 rdzenie przy lekkim obciążeniu; szczytowo 2–4 rdzenie przy intensywnym sniffie.
RAM: 150–400 MB.
Bezpieczeństwo:
Klucz trzymany poza repo; zalecane: HSM/remote signer/in-memory z ograniczeniami dostępu.
Ochrona przed replay/duplicate: dedup sygnatur, monitoring mempool.
Uwagi:
- Budowa prawdziwych transakcji to TODO — obecnie placeholder tx.


## Konfiguracja (config.toml)

```toml
sniffer_mode = "real"

# WSS watchdog + reconnect
wss_required = false
wss_heartbeat_ms = 1500
wss_reconnect_backoff_ms = 500
wss_reconnect_backoff_max_ms = 10000
wss_max_silent_ms = 5000

# HTTP fallback
http_fallback_enabled = true
http_poll_interval_ms = 1000
http_sig_depth = 50
http_max_parallel_tx_fetch = 6

# Metadata backfill
meta_fetch_enabled = true
meta_fetch_commitment = "confirmed"

# Endpoints
rpc_endpoints = ["https://api.mainnet-beta.solana.com"]
rpc_wss_endpoints = ["wss://api.mainnet-beta.solana.com"]

# Engine/GUI
nonce_count = 5
gui_update_interval_ms = 200
# keypair_path = "/home/user/.config/solana/id.json"
```

ENV override dla trybu:
```bash
SNIFFER_MODE=mock cargo run
SNIFFER_MODE=real cargo run
```

Poziom logów:
```bash
RUST_LOG=info cargo run
RUST_LOG=sniffer=debug,engine=debug cargo run
```

## GUI

- Wyświetla stan: Sniffing/Passive, mint, cena zakupu (mock), holdings
- Przyciski i skróty: S=100%, Q=50%, W=25%

## TESTY:

```bash
cargo test
```

## STRUKTURA:

BOT:
- /src:
   - /sniffer:
    - http_source.rs
    - real.rs
    - runner.rs
    - source.rs
    - wss_source.rs
  - sniffer.rs   // mock, real utils, źródła WSS/HTTP, runner (failover).
  - lib.rs  
  - types.rs    // typy AppState, PremintCandidate, ProgramLogEvent.
  - config.rs   // konfiguracja + ENV override.
  - main.rs   // spina resztę elementów w działający program. 
  - time_utils.rs   // helper czasu.
  - buy_engine.rs   // logika one-slot + SELL API
  - rpc_manager.rs   // RpcBroadcaster trait + produkcyjny RpcManager
  - nonce_manager.rs   // lekki menedżer pseudo-nonce (semafor/indeksy)
  - candidate_buffer.rs 
  - gui.rs   // prosty eframe/egui GUI
- /tests:
  - buy_flow_mock.rs
  - sniffer_runner_mock.rs
- config.example.rs — konfiguracja + ENV override
- Cargo.toml

## ZASADA DZIAŁANIA POSZCZEGÓLNYCH KOMPONENTÓW:

Oczekiwane, docelowe działanie bota w wersji produkcyjnej – po uzupełnieniu placeholderów i braków – wraz z funkcjonalnością, parametrami i szacunkami czasów.

Funkcjonalność (high level)

Sniffer (REAL):
 - Subskrypcja WebSocket, oraz w późniejszym etapie Geyser/Jito/ (program logs + mempool) dla źródeł: pump.fun oraz bonk.fun (i ewentualnie inne).
 - Parsowanie zdarzeń do PremintCandidate, heurystyki filtrujące (program, wolumen, liczba mintów danej kolekcji, sygnatury kreatora).
 - Reconnect + backoff z jitterem, health checks, metryki opóźnień.

# Candidate_buffer:

Cel
Bufor kandydatów (PremintCandidate) z funkcją TTL (Time-To-Live) i eliminacją duplikatów.
Zapewnia:

Przechowywanie tylko unikalnych kandydatów (po mint)
Automatyczne usuwanie przeterminowanych wpisów
Prosta polityka wyboru „najlepszego” kandydata (najstarszy, czyli najwcześniej dodany)
Ograniczenie ilości wpisów (max_size); w razie przepełnienia usuwa najstarszy
Struktura danych
map: HashMap<Pubkey, (PremintCandidate, Instant)>
Bufor przechowuje kandydatów pod kluczem mint (Pubkey), wraz z czasem dodania (Instant).

ttl: Duration
Maksymalny czas życia wpisu (od dodania).

max_size: usize
Maksymalna liczba wpisów w buforze.

Główne operacje
1. push(c: PremintCandidate) -> bool
Czyści bufor z przeterminowanych wpisów (cleanup)
Jeśli kandydat o danym mint już istnieje — ignoruje (duplikat)
Jeśli bufor pełny — usuwa najstarszy wpis
Dodaje nowego kandydata
Zwraca true gdy dodano, false gdy duplikat/odrzucono
2. pop_best() -> Option<PremintCandidate>
Czyści bufor z przeterminowanych wpisów
Wybiera najstarszego (najwcześniej dodanego) kandydata
Usuwa go z bufora, zwraca
Jeśli bufor pusty — zwraca None
3. cleanup() -> usize
Usuwa wszystkie przeterminowane wpisy (wg ttl)
Zwraca liczbę usuniętych
4. Wersja współdzielona:
Bufor można udostępniać jako Arc<Mutex<CandidateBuffer> — bezpieczny dostęp współbieżny.
5. new_shared(ttl, max_size) -> SharedCandidateBuffer
Tworzy współdzielony bufor w Arc+Mutex
Polityka wyboru „najlepszego” kandydata
Zawsze wybierany jest najstarszy wpis (najwcześniej dodany, wg Instant).
Ochrona przed duplikatami
Każdy mint (Pubkey) może pojawić się tylko raz.
Próba dodania duplikatu ignorowana.
TTL — zarządzanie czasem życia
Wpisy wygasają po czasie ttl — usuwane automatycznie przy każdej operacji push/pop oraz przez manualne wywołanie cleanup.
Jeśli ttl = 0 — wszystko wygasa natychmiast.
Zarządzanie pojemnością
Jeśli bufor osiągnie max_size, najstarszy wpis jest usuwany przy próbie dodania kolejnego.

# BuyEngine (jednotokenowy stan):

Cel i funkcje
BuyEngine to kluczowy komponent automatyzujący proces kupna tokenów w trybie sniffera, przechodząc do trybu posiadania jednego tokena (PassiveToken), a następnie pozwalający na sprzedaż i powrót do sniffera.

Główne zadania:

Pobiera kandydatów do kupna z kanału (CandidateReceiver) – czyli tokeny do rozważenia zakupu.
Filtruje kandydatów prostą heurystyką (program == "pump.fun").
Przeprowadza próbę kupna (pozyskuje N nonce’ów, buduje N transakcji, broadcastuje je przez RpcBroadcaster).
Po pierwszym udanym kupnie przechodzi w tryb PassiveToken, trzymając jeden token aż do sprzedaży.
Udostępnia API sprzedaży (sell(percent)), które redukuje stan posiadania i wraca do sniffera po pełnej sprzedaży.
Stan wewnętrzny (AppState)
Tryb pracy (Mode): Sniffing (szukanie nowych tokenów) lub PassiveToken (trzymanie kupionego tokena).
active_token: Obiekt tokena, który został kupiony.
last_buy_price: Ostatnia cena zakupu (mockowana).
holdings_percent: Procentowy udział posiadania tokena (od 0 do 1).
Główna pętla (run)
Sprawdza, czy jest w trybie sniffingu.
Jeśli tak:
Odbiera kandydata z kanału (timeout 1000ms).
Filtruje kandydata (musi mieć program pump.fun).
Próbuje kupić:
Pozyskuje nonces, buduje transakcje, broadcastuje przez RPC.
Po sukcesie przechodzi w tryb PassiveToken, zapamiętuje dane tokena.
Po niepowodzeniu zostaje w sniffingu.
Jeśli kanał zamknięty: wychodzi z pętli.
Jeśli nie:
Jest w trybie PassiveToken – ignoruje kandydatów (timeout 500ms, sleep 50ms).
Kupno (try_buy)
Pozyskuje do N nonce’ów (wg configu).
Dla każdego nonce buduje szkieletową transakcję (placeholder, demo).
Wysyła wszystkie transakcje przez RPC.
Po zakończeniu zwalnia nonce’y.
Zwraca podpis (Signature) lub błąd.
Sprzedaż (sell(percent))
Pozwala sprzedać określony procent posiadanych tokenów (clamp 0.0–1.0).
Buduje transakcję sprzedaży (placeholder).
Wysyła przez RPC.
Aktualizuje holdings_percent.
Jeśli holdings_percent ≤ 0:
Wraca do trybu sniffingu, czyści stan tokena.
Obsługuje błędy broadcastu.
Heurystyka wyboru kandydata (is_candidate_interesting)
Filtruje tylko na podstawie programu: akceptuje tylko, jeśli program == "pump.fun".
Mockowane funkcje
get_execution_price_mock – zwraca zawsze cenę 1.0 (do testów).
create_placeholder_tx – tworzy przykładową transakcję transferu SOL (do testów/demo).
Testy jednostkowe
Testują cykl kupna (po wrzuceniu kandydata engine przechodzi w PassiveToken) i sprzedaży (po sprzedaży wraca do sniffingu).
Podsumowanie — cykl działania
Sniffing:

Oczekuje na ciekawych kandydatów z kanału.
Próbkuje kupno.
Po sukcesie przechodzi w tryb PassiveToken.
PassiveToken:

Trzyma jeden token.
Ignoruje kolejne kandydaty.
Pozwala na sprzedaż części/całości.
Powrót do sniffingu:

Po pełnej sprzedaży wraca do sniffera.


# RpcManager (Improved):

Cel
Zapewnia asynchroniczne, równoległe rozsyłanie transakcji (VersionedTransaction) do wielu endpointów RPC Solana z zaawansowanymi strategiami broadcastu, adaptacyjnym rankingowaniem endpointów i inteligentną obsługą błędów.
Pozwala na łatwe mockowanie w testach dzięki traitowi RpcBroadcaster.

**ULEPSZONE FUNKCJONALNOŚCI:**
✅ Naprawione sztywne 1:1 pairing - dodano strategie broadcast
✅ Adaptacyjne rankingowanie endpointów na podstawie latencji i success rate
✅ Konfigurowalne timeouty zamiast stałych 8s
✅ Naprawiona spójność commitment (Confirmed vs Processed mismatch)
✅ Wczesne anulowanie przy błędach krytycznych (early-cancel policy)
✅ Cache RpcClient - unika TLS handshake overhead
✅ Lepsza redundancja - wykorzystuje endpoints > txs

Główne elementy
1. Trait: RpcBroadcaster
Rust
pub trait RpcBroadcaster: Send + Sync {
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>>;
}

Definiuje interfejs do broadcastu listy transakcji i zwraca pierwszy udany Signature lub błąd.

2. Produkcyjna implementacja: RpcManager (Ulepszona)
Rust
pub struct RpcManager {
    pub endpoints: Vec<String>,
    pub config: Config,
    // Cached RPC clients - unika TLS overhead
    clients: Arc<RwLock<HashMap<String, Arc<RpcClient>>>>,
    // Metryki wydajności dla adaptacyjnego rankingu
    metrics: Arc<RwLock<HashMap<String, EndpointMetrics>>>,
}

Konstruktory:
- `new(endpoints)` - domyślny config
- `new_with_config(endpoints, config)` - pełna kontrola

3. Strategie Broadcastu (BroadcastMode)
```toml
broadcast_mode = "pairwise"    # Sztywne 1:1 (oryginał)
broadcast_mode = "replicate"   # Najlepsza TX → wszystkie endpoints 
broadcast_mode = "round_robin" # TX równomiernie po endpoints
broadcast_mode = "full_fanout" # Każda TX → każdy endpoint
```

4. Adaptacyjne Rankingowanie
EndpointMetrics track:
- success_rate = sukces/(sukces+błąd)  
- avg_latency_ms = średnia latencja
- score = success_rate * (1000/latency) 

Najlepsze endpointy używane pierwsze.

5. Wczesne Anulowanie
```toml
early_cancel_threshold = 2  # Anuluj po 2 błędach krytycznych
```
Błędy krytyczne: "BlockhashNotFound", "TransactionExpired", "AlreadyProcessed"

6. Konfigurowalne Timeouty  
```toml
rpc_timeout_sec = 8  # Domyślnie 8s, ale konfigurowalne
```

Obsługa błędów
Wszystkie błędy logowane i uwzględniane w metrykach adaptacyjnych.
Wczesne anulowanie oszczędza czas przy błędach wskazujących na wygaśnięte transakcje.

Przykładowe zastosowanie
- **Replicate mode**: Pojedyncza SELL transakcja wysłana do wszystkich 6 endpointów
- **Round-robin**: 10 transakcji równomiernie po 3 endpointach  
- **Adaptacyjny ranking**: Wolne/niestabilne endpointy automatycznie depriorytetyzowane
- **Wczesne anulowanie**: Oszczędza czas gdy wszystkie TX są już wygasłe

Podsumowanie  
Zaawansowany RpcManager rozwiązuje wszystkie problemy wysokiego priorytetu:
redundancje, adaptacyjność, konfigurowalność, spójność, wczesne anulowanie i cache.

Cel
Zapewnia asynchroniczne, równoległe rozsyłanie transakcji (VersionedTransaction) do wielu endpointów RPC Solana oraz obsługę sukcesu/błędów i timeoutów.
Pozwala na łatwe mockowanie w testach dzięki traitowi RpcBroadcaster.

Główne elementy
1. Trait: RpcBroadcaster
Rust
pub trait RpcBroadcaster: Send + Sync {
    fn send_on_many_rpc<'a>(
        &'a self,
        txs: Vec<VersionedTransaction>,
    ) -> Pin<Box<dyn Future<Output = Result<Signature>> + Send + 'a>>;
}

Definiuje interfejs do broadcastu listy transakcji i zwraca pierwszy udany Signature lub błąd.
Pozwala na produkcyjną i mockowaną implementację.

2. Produkcyjna implementacja: RpcManager
Rust
#[derive(Debug, Clone)]
pub struct RpcManager {
    pub endpoints: Vec<String>,
}
Przechowuje listę endpointów RPC (adresy HTTP node’ów Solana).
Konstruktor
Rust
pub fn new(endpoints: Vec<String>) -> Self
Tworzy manager z podanymi endpointami.

3. Implementacja broadcastu
send_on_many_rpc (async)
Dla każdej transakcji (do n, gdzie n = min(endpointów, transakcji)):
Tworzy klienta RPC dla danego endpointa.
Wysyła transakcję z configiem (skip preflight, max_retries, odpowiedni commitment).
Ustawia timeout (8 sek.).
Loguje sukces/błąd/timeout.
Wszystkie wysyłki odbywają się równolegle (tokio::JoinSet).
Gdy pierwsza transakcja zostanie wysłana pomyślnie — abortuje resztę i zwraca jej Signature.
Jeśli wszystkie próby zawiodą — zwraca błąd.
Obsługa błędów
Timeout, błąd RPC, join error — każdy przypadek jest logowany i obsługiwany.
Polityka
Maksymalna liczba równoległych wysyłek = min(endpointów, transakcji).
Sukces = pierwszy udany Signature.
Błąd = gdy wszystkie próby zawiodą.
Przykładowe zastosowanie
Pozwala na broadcasting transakcji z bufora do wielu node’ów Solana — szansa na szybszy inclusion w blok, redundancja sieciowa.
Umożliwia "failover": jeśli jeden endpoint nie działa, inne mogą zadziałać.
Podsumowanie
rpc_manager.rs to produkcyjny broadcast transakcji do wielu endpointów Solana z obsługą timeoutów, równoległości, zwracaniem pierwszego sukcesu i pełną obsługą błędów.
Zapewnia prosty interfejs do mockowania w testach.

# NonceManager

Cel:
Zarządzanie slotami nonce, czyli unikalnymi punktami wejścia dla równoległych transakcji na Solanie, by uniknąć duplikacji, blokad sieci i podnieść skuteczność snajpingu.

Funkcjonalność:

Pool nonce:
Inicjuje pulę (np. 8–16) slotów durable-nonce, z których każdy pozwala na zbudowanie i wysłanie transakcji niezależnie, bez ryzyka “nonce too old” czy “blockhash not found”.

Allocation:
Przy każdym “BUY” lub “SELL” engine pyta NonceManager o wolny slot.
Po pobraniu slotu engine buduje transakcję z danym nonce i podpisuje ją.

Release & Rotate:
Po potwierdzeniu (lub failu) transakcji slot wraca do puli.
Jeśli slot się zestarzeje (nonce wygasł), NonceManager automatycznie odświeża go (np. przez “advance nonce” transakcję).

Fallback:
Jeśli pula nonce jest niedostępna (np. zator, błąd RPC), przełącza strategię:

priority fee + świeży blockhash (mniej bezpieczne, ale szybkie).
Integracja z Jito bundle/tips (opcjonalnie).
Concurrency:
Pozwala na równoległe emitowanie wielu transakcji (N równoległych prób BUY/SWAP), bez ryzyka “nonce collision”.

Health checks:
Monitoruje świeżość nonce, czas życia slotu, liczbę wykorzystanych slotów, automatyczne odświeżanie.

Integracja:
Engine/BuyEngine korzysta z NonceManager przez prosty interfejs (get_nonce, release_nonce, refresh_nonce).
Możliwość plugowania HSM/remote signer, jeśli bezpieczeństwo klucza wymagane.

Observability:
Eksportuje metryki:

średni czas życia nonce slotu
liczba odświeżeń
liczba błędów “nonce too old”
QPS alloc/release

Parametry:

nonce_count: liczba slotów w puli (np. 3-6)
nonce_refresh_interval_ms: czas po którym slot jest odświeżany
nonce_failover_enabled: czy przełączać na blockhash+priority-fee jeśli pula się skończy
rpc_endpoints: lista endpointów do obsługi nonce (może być rozdzielna od głównej puli RPC)
keypair_path: ścieżka do klucza zarządzającego nonce accountem


# GUI (eframe/egui):

Cel: 
Plik gui.rs udostępnia prosty interfejs graficzny (GUI) dla sniffer-bota, umożliwiający podgląd stanu bot-a oraz sprzedaż tokenów przez kliknięcia lub skróty klawiszowe.

Główne elementy
1. GuiEvent
Rust
#[derive(Clone, Debug)]
pub enum GuiEvent {
    SellPercent(f64),
}
pub type GuiEventSender = Sender<GuiEvent>;
GuiEvent — typ komunikatu wysyłanego z GUI do logicznej warstwy bota (np. żądanie sprzedaży procentowej).
GuiEventSender — kanał do wysyłania tych zdarzeń.

2. launch_gui
Rust
pub fn launch_gui(
    title: &str,
    app_state: Arc<Mutex<AppState>>,
    gui_tx: GuiEventSender,
    refresh: Duration,
) -> Result<()>
Uruchamia natywną aplikację GUI przez eframe/egui.
Przekazuje referencję do stanu aplikacji (AppState), kanał zdarzeń i czas odświeżania GUI.

3. BotApp
Struktura przechowująca referencje do stanu bota, kanału zdarzeń i interwału odświeżania.
Implementuje logikę GUI, w tym renderowanie stanu i obsługę interakcji.

4. Renderowanie stanu: draw_state
Rust
fn draw_state(&self, ui: &mut egui::Ui, st: &AppState)
Wyświetla nagłówek ("Sniffer Bot").
Pokazuje tryb pracy (Sniffing lub PassiveToken z adresem mint).
Pokazuje informacje o aktywnym tokenie (mint, ostatnia cena kupna, procent posiadanych tokenów).
Wyświetla przyciski do sprzedaży 25%, 50% i 100% (oraz skróty klawiszowe W/Q/S).

5. Obsługa interakcji
Przyciski:
Kliknięcie przycisku "Sell X%" wysyła odpowiedni komunikat przez kanał GuiEventSender.
Klawisze:
Naciśnięcie klawisza W/Q/S wywołuje sprzedaż odpowiedniego procentu (25/50/100%).

6. Główna pętla GUI (App for BotApp)
Rust
impl App for BotApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame)
Sprawdza, czy wciśnięto klawisze W/Q/S i wysyła eventy sprzedaży.

Renderuje centralny panel z aktualnym stanem (pobiera go przez blocking_lock z Mutexa).

Ustawia odświeżanie GUI wg podanego interwału.

on_close_event:
Loguje zamknięcie GUI.

Podsumowanie
GUI pozwala na podgląd aktualnego stanu bota oraz sprzedaż tokenów w wybranych procentach.
Interfejs jest prosty (nagłówek, info o tokenie, przyciski, skróty).
Komunikacja z logiką bota odbywa się przez kanał zdarzeń.

Konfiguracja i operacje:
 - TOML + ENV override (sniffer_mode, rpc_endpoints, nonce_count, progi wolumenu, fee strategy).
Bezpieczne zarządzanie kluczem (ścieżka do keypair, opcjonalnie HSM/remote signer).
Monitoring/observability:
Tracing + Prometheus (latencje: sniff, build, send, confirm; skuteczność endpointów; QPS kandydatów).
Alerty (brak kandydatów X minut, 0% skuteczności, wysokie czasy potwierdzeń).
Testy i niezawodność:
E2E na solana-test-validator + lokalne mocki Geyser.
Testy regresyjne scenariuszy: brak sieci, zator RPC, wygaśnięty blockhash, partial fills.
Przybliżone parametry i docelowe metryki

# Sniffer:

Cel
Plik ten jest entrypointem dla sniffera, czyli procesu wykrywającego nowe premint tokeny.
Koordynuje uruchamianie sniffera w trybie Mock (symulacja na potrzeby testów/demo) oraz Real (prawdziwe źródła: WSS + HTTP fallback).
- Subskrypcja WebSocket, oraz w późniejszym etapie Geyser/Jito/ (program logs + mempool) dla źródeł: pump.fun oraz bonk.fun (i ewentualnie inne). 
- Parsowanie zdarzeń do PremintCandidate, heurystyki filtrujące (program, wolumen, liczba mintów danej kolekcji, sygnatury kreatora). 
- Reconnect + backoff z jitterem, health checks, metryki opóźnień.


Kluczowe stałe
CANDIDATE_TTL — przez ile sekund ignorować powtórzenia tego samego mint-a (antyduplikacja).
DEBOUNCE_DELAY — minimalny odstęp między emisjami kandydatów (anty-wielokrotna emisja).
MAX_CANDIDATE_AGE — maksymalny dopuszczalny wiek kandydata na podstawie jego timestampu.
Główne funkcje
1. run_sniffer
Rust
pub async fn run_sniffer(mode: SnifferMode, sender: CandidateSender, config: &Config) -> JoinHandle<()> 
Uruchamia sniffer w zależności od trybu:
Mock: run_mock_sniffer
Real: Tworzy SnifferRunner i uruchamia go jako task z przekazanym kanałem sendera.
2. run_mock_sniffer
Rust
pub fn run_mock_sniffer(sender: CandidateSender) -> JoinHandle<()>
Tworzy asynchroniczny task generujący "fałszywe" PremintCandidate.
Stosuje logikę TTL, debounce i age:
TTL: Każdy mint ignorowany jeśli pojawi się w oknie TTL.
Debounce: Emituje kandydata nie częściej niż co DEBOUNCE_DELAY.
Age: (w mocku zawsze 0) — w realu odrzuca zbyt stare.
Burst: co pewien czas generuje "burst" 3 kandydatów.
Wysyła kandydatów przez kanał do dalszej warstwy logiki.
W razie błędu kanału kończy task.
3. Importowane moduły
mod real/source/wss_source/http_source/runner — implementacje prawdziwych źródeł sniffera (WSS, HTTP, fallback, runner).
PremintCandidate, CandidateSender — typy kandydata i kanału.
Config, SnifferMode — konfiguracja i tryb pracy.
Polityka antyduplikacyjna i filtrowania
Duplikaty: Zapamiętuje minty na czas TTL, odrzuca powtórzenia.
Debounce: Nie emituje zbyt często, minimalny odstęp.
Age: (w mocku nieaktywny, w realu istotny) — odrzuca zbyt stare kandydaty.
Tryb Mock vs Real
Mock:
Emituje sztuczne kandydaty dla testów GUI i logiki bota.
Umożliwia szybkie testowanie bez prawdziwych danych.
Real:
Pełna implementacja w plikach runner/real/source/wss_source/http_source.
Integracja z prawdziwymi źródłami Solana.
Podsumowanie
sniffer.rs jest punktem wyjścia dla całego procesu sniffera, uruchamiając go w trybie testowym lub produkcyjnym.
Zapewnia ochronę przed spamem, duplikatami i zbyt starymi kandydatami.
Mock pozwala na wygodne testowanie GUI i logiki.


OCZEKIWANE PARAMETRY: 

Latencja detekcji od zdarzenia do kandydata: 30–150 ms (zależnie od feedu i hostingu).
Throughput: dziesiątki–setki kandydatów/min (filtr redukuje do < 10/min istotnych).
Bufor:
Rozmiar: 512–2048 pozycji.
TTL: 15–60 s (konfigurowalne).
Buy path:
Budowa 8–16 wariantów TX (nonce/prio): 5–25 ms łącznie.
Broadcast do 3–6 RPC równolegle; pierwszy sukces: typowo 150–700 ms.
Timeout per send: 4–8 s; retries: 1–3; skip_preflight: true.
Skuteczność “first success”: > 90% przy poprawnych endpointach i adequate fee.
SELL:
Manual trigger natychmiastowy; TX build ~5–15 ms; broadcast jak wyżej.
GUI:
Odświeżanie: 100–250 ms.
Zużycie zasobów (szacunek, jedna instancja):
CPU: 1–2 rdzenie przy lekkim obciążeniu; szczytowo 2–4 rdzenie przy intensywnym sniffie.
RAM: 150–400 MB.
Bezpieczeństwo:
Klucz trzymany poza repo; zalecane: HSM/remote signer/in-memory z ograniczeniami dostępu.
Ochrona przed replay/duplicate: dedup sygnatur, monitoring mempool.
Uwagi:
- Budowa prawdziwych transakcji to TODO — obecnie placeholder tx.

