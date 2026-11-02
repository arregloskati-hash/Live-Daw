//! The console's one RTA for remote clients: WING Edit shows the RTA of the
//! strip it shows only when `/cfg/rta/$src` is that strip ("RTA in use!
//! Check mixer settings" otherwise) and draws the RTA meter's 120 bands.
//! `$src` follows RTA SOURCE (`rtasrc`, 0 SEL: the selected strip,
//! `/$ctl/$stat/selidx`), or the screens' RTA while they show one.

use livedaw_engine::{Engine, StripKind};
use livedaw_wing::meters::{self, Meter, MeterClock};
use livedaw_wing::schema::NoModels;
use livedaw_wing::{schema, Console, Value, WingState};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

fn engine() -> Engine {
    let e = Engine::new(0);
    e.set_num_channels(40);
    e
}

fn node(path: &str) -> livedaw_wing::Node {
    schema().resolve(path, &NoModels).unwrap_or_else(|| panic!("{path}"))
}

fn get(c: &Console, path: &str) -> Option<Value> {
    c.get(node(path))
}

fn set(c: &Console, path: &str, v: Value) {
    c.set(node(path), &v).unwrap_or_else(|e| panic!("{path}: {e:?}"));
}

fn select(e: &Engine, st: &WingState, kind: StripKind, i: usize) {
    st.set_selected(Some((kind, e.strip_at(kind, i).unwrap().id())));
}

#[test]
fn the_rta_source_follows_the_selection() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    // SEL by default: the selected strip's number (channels 1-40, aux 41-48,
    // buses 49-64, mains 65-68, matrices 69-76); selidx reads it 0-based.
    assert_eq!(get(&c, "/cfg/rta/rtasrc"), Some(Value::Int(0)));
    select(&e, &st, StripKind::Channel, 0);
    assert_eq!(get(&c, "/$ctl/$stat/selidx"), Some(Value::Int(0)));
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(1)));
    select(&e, &st, StripKind::Channel, 7);
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(8)));
    select(&e, &st, StripKind::Bus, 0);
    assert_eq!(get(&c, "/$ctl/$stat/selidx"), Some(Value::Int(48)));
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(49)));
    // A fixed RTA SOURCE is the RTA's strip (and selects it).
    set(&c, "/cfg/rta/rtasrc", Value::Int(3));
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(3)));
    assert_eq!(get(&c, "/$ctl/$stat/selidx"), Some(Value::Int(2)));
    // Back to SEL: the selection.
    set(&c, "/cfg/rta/rtasrc", Value::Int(0));
    select(&e, &st, StripKind::Bus, 0);
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(49)));
}

#[test]
fn a_client_selecting_a_strip_asks_the_host() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    set(&c, "/$ctl/$stat/selidx", Value::Int(5));
    let ch5 = e.strip_at(StripKind::Channel, 4).unwrap().id();
    assert_eq!(st.take_select_request(), Some((StripKind::Channel, ch5)));
    assert_eq!(st.take_select_request(), None);
    assert_eq!(get(&c, "/$ctl/$stat/selidx"), Some(Value::Int(4)));
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(5)));
    // Past the range: the last strip (matrix 8), as the console clamps.
    set(&c, "/$ctl/$stat/selidx", Value::Int(200));
    assert_eq!(get(&c, "/$ctl/$stat/selidx"), Some(Value::Int(75)));
}

#[test]
fn the_screens_rta_is_the_source_while_shown() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    // Channel 12 selected; the screens show its RTA (HOME, as the UI does).
    select(&e, &st, StripKind::Channel, 11);
    let a = e.analysis().controls();
    a.strip_kind.store(StripKind::Channel as i32, Relaxed);
    a.strip_id.store(e.strip_at(StripKind::Channel, 11).unwrap().id(), Relaxed);
    a.rta_on.store(true, Relaxed);
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(12)));
    // The meter stream reads it, and leaves the screens' choice alone.
    let words = meters::rta_words(&c);
    assert_eq!(words.len(), 120);
    assert_eq!(a.strip_id.load(Relaxed), e.strip_at(StripKind::Channel, 11).unwrap().id());
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(12)));
}

#[test]
fn the_meter_stream_turns_the_rta_on_for_its_source_and_off_when_idle() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    select(&e, &st, StripKind::Channel, 2);
    let a = e.analysis().controls();
    assert!(!a.rta_on.load(Relaxed));
    let words = meters::rta_words(&c);
    assert_eq!(words.len(), 120);
    // On, for the selected channel 3; $src says so.
    assert!(a.rta_on.load(Relaxed));
    assert_eq!(a.strip_id.load(Relaxed), e.strip_at(StripKind::Channel, 2).unwrap().id());
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(3)));
    // The selection moves: the RTA follows on the next frame.
    select(&e, &st, StripKind::Channel, 5);
    meters::rta_words(&c);
    assert_eq!(a.strip_id.load(Relaxed), e.strip_at(StripKind::Channel, 5).unwrap().id());
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(6)));
    // Read lately: stays on.
    meters::release_idle_rta(&e, &st);
    assert!(a.rta_on.load(Relaxed));
    // Nobody reads it for RTA_IDLE: off again.
    let deadline = Instant::now() + meters::RTA_IDLE + Duration::from_millis(300);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    meters::release_idle_rta(&e, &st);
    assert!(!a.rta_on.load(Relaxed));
}

#[test]
fn the_rta_meter_is_part_of_a_frame() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    select(&e, &st, StripKind::Channel, 0);
    let mut clock = MeterClock::default();
    clock.tick(&e, Instant::now());
    let words = meters::frame_words(&c, &clock, &[Meter::Indexed(0xab, 0), Meter::Rta]);
    assert_eq!(words.len(), Meter::Indexed(0xab, 0).words() + 120);
    // Silence: the bottom (0) in every band (no audio runs in this test).
    assert!(words[words.len() - 120..].iter().all(|&w| w == 0));
}

#[test]
fn rta_words_are_in_the_consoles_scale() {
    // 1/256 dB above -128 dB: full scale at the top, silence at 0.
    assert_eq!(meters::rta_word(0.0), i16::MAX);
    assert_eq!(meters::rta_word(12.0), i16::MAX);
    assert_eq!(meters::rta_word(-30.0), (98 * 256) as i16);
    assert_eq!(meters::rta_word(-200.0), 0);
    assert_eq!(meters::rta_word(f32::NAN), 0);
}

#[test]
fn autogain_puts_the_average_mid_range() {
    let st = WingState::default();
    // 30 dB RANGE: an average of -40 dB goes to -15 dB (mid-range under the top).
    assert_eq!(st.rta_autogain(-40.0, 30.0), 25.0);
    // Never more than the console's 50 dB.
    let st2 = WingState::default();
    assert_eq!(st2.rta_autogain(-100.0, 30.0), 50.0);
    // Silence: no gain, eased toward it.
    assert!(st.rta_autogain(-144.0, 30.0) < 25.0);
}

#[test]
fn the_tap_is_the_claimed_rta_tap() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    assert_eq!(get(&c, "/cfg/rta/$tap"), Some(Value::str("IN")));
    // WING Edit showing channel 1's EQ writes RTA SOURCE 1 and TAP PREEQ.
    set(&c, "/cfg/rta/rtasrc", Value::Int(1));
    set(&c, "/cfg/rta/rtatap", Value::str("PREEQ"));
    assert_eq!(get(&c, "/cfg/rta/$tap"), Some(Value::str("PREEQ")));
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(1)));
}

#[test]
fn the_remote_rta_uses_the_eq_rta_settings() {
    // WING Edit's EQ/RTA SETTINGS write /cfg/rta/eq*; the stream analyses
    // with them (view EQ), not with the HOME RTA's.
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    select(&e, &st, StripKind::Channel, 0);
    set(&c, "/cfg/rta/eqauto", Value::Int(1));
    set(&c, "/cfg/rta/rtaauto", Value::Int(0));
    meters::rta_words(&c);
    let a = e.analysis().controls();
    assert_eq!(a.view.load(Relaxed), livedaw_engine::analyser::rta_view::EQ);
    assert!(a.view_settings().autogain.load(Relaxed));
}

#[test]
fn the_stream_shares_the_rta_with_a_screen_instead_of_fighting_over_it() {
    use livedaw_engine::analyser::rta_view;
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    select(&e, &st, StripKind::Channel, 0);
    let a = e.analysis().controls();
    // The stream turns it on (EQ view) ...
    meters::rta_words(&c);
    assert_eq!(a.view.load(Relaxed), rta_view::EQ);
    // ... then a screen shows its HOME RTA of the same channel: it keeps it.
    a.view.store(rta_view::HOME, Relaxed);
    for _ in 0..5 {
        meters::rta_words(&c);
        assert_eq!(a.view.load(Relaxed), rta_view::HOME);
        assert!(a.rta_on.load(Relaxed));
    }
    // And going idle doesn't turn the screen's RTA off.
    meters::release_idle_rta(&e, &st);
    assert!(a.rta_on.load(Relaxed));
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(1)));
}

#[test]
fn the_rta_sits_where_wing_edit_reads_it() {
    // WING Edit's EQ page request: 16 channels V2, main 1 V2, sources LCL,
    // B, MOD, MAIN, RTA. Its RTA starts 339 + 112 - 120 words in (measured).
    let ms: Vec<Meter> = (0..16)
        .map(|i| Meter::Indexed(0xab, i))
        .chain([Meter::Indexed(0xae, 0)])
        .chain([0u8, 3, 8, 14].map(|i| Meter::Indexed(0xa7, i)))
        .chain([Meter::Rta])
        .collect();
    let before: usize = ms[..ms.len() - 1].iter().map(|m| m.words()).sum();
    assert_eq!(before, 331);
    assert_eq!(ms.iter().map(|m| m.words()).sum::<usize>(), 451);
}

#[test]
fn the_rta_follows_the_strip_being_looked_at() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    select(&e, &st, StripKind::Channel, 0);
    // WING Edit shows bus 3's EQ: RTA SOURCE 51; Sidecar selects bus 3.
    set(&c, "/cfg/rta/rtasrc", Value::Int(51));
    let bus3 = (StripKind::Bus, e.strip_at(StripKind::Bus, 2).unwrap().id());
    assert_eq!(st.take_select_request(), Some(bus3));
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(51)));
    // Channel 7 selected (on Sidecar or in WING Edit): the RTA follows it.
    select(&e, &st, StripKind::Channel, 6);
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(7)));
    meters::rta_words(&c);
    let a = e.analysis().controls();
    assert_eq!(a.strip_id.load(Relaxed), e.strip_at(StripKind::Channel, 6).unwrap().id());
    // Main 1, matrix 2: each its own.
    set(&c, "/cfg/rta/rtasrc", Value::Int(65));
    meters::rta_words(&c);
    assert_eq!(a.strip_id.load(Relaxed), e.strip_at(StripKind::Main, 0).unwrap().id());
    set(&c, "/$ctl/$stat/selidx", Value::Int(70));
    meters::rta_words(&c);
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(70)));
    assert_eq!(a.strip_id.load(Relaxed), e.strip_at(StripKind::Matrix, 1).unwrap().id());
}

#[test]
fn a_screen_rta_left_on_another_strip_does_not_hold_the_rta() {
    let e = engine();
    let st = WingState::default();
    let c = Console::new(&e, &st);
    // A screen RTA of channel 4 left on; channel 2 selected (WING Edit's).
    let a = e.analysis().controls();
    a.strip_kind.store(StripKind::Channel as i32, Relaxed);
    a.strip_id.store(e.strip_at(StripKind::Channel, 3).unwrap().id(), Relaxed);
    a.rta_on.store(true, Relaxed);
    select(&e, &st, StripKind::Channel, 1);
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(2)));
    meters::rta_words(&c);
    assert_eq!(a.strip_id.load(Relaxed), e.strip_at(StripKind::Channel, 1).unwrap().id());
    assert_eq!(get(&c, "/cfg/rta/$src"), Some(Value::Int(2)));
}
