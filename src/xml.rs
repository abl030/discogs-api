use quick_xml::events::Event;
use quick_xml::Reader;
use std::io::BufRead;

use crate::types::*;

fn get_attr(e: &quick_xml::events::BytesStart, name: &[u8]) -> String {
    e.attributes()
        .flatten()
        .find(|a| a.key.as_ref() == name)
        .and_then(|a| a.unescape_value().ok())
        .map(|v| v.into_owned())
        .unwrap_or_default()
}

fn get_attr_i32(e: &quick_xml::events::BytesStart, name: &[u8]) -> i32 {
    get_attr(e, name).parse().unwrap_or(0)
}

fn parse_artist_field(tag: &[u8], text: &str, artist: &mut CreditedArtist) {
    match tag {
        b"id" => artist.artist_id = text.parse().unwrap_or(0),
        b"name" => artist.artist_name = text.to_string(),
        b"anv" => artist.anv = text.to_string(),
        b"role" => artist.role = text.to_string(),
        b"join" => artist.join_relation = text.to_string(),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Artists
// ---------------------------------------------------------------------------

pub fn parse_artists(
    reader: impl BufRead,
    mut on_entity: impl FnMut(Artist),
) -> anyhow::Result<usize> {
    #[derive(PartialEq)]
    enum Section { Root, NameVars, Aliases }

    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut count = 0usize;
    let mut current: Option<Artist> = None;
    let mut section = Section::Root;
    let mut current_alias = ArtistAlias::default();
    let mut text_buf = String::new();
    let mut skip_depth = 0u32;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                text_buf.clear();
                if skip_depth > 0 { skip_depth += 1; buf.clear(); continue; }
                let tag = e.name();
                match tag.as_ref() {
                    b"artist" if current.is_none() => {
                        current = Some(Artist::default());
                        section = Section::Root;
                    }
                    b"namevariations" if current.is_some() => section = Section::NameVars,
                    b"aliases" if current.is_some() => section = Section::Aliases,
                    b"name" if section == Section::Aliases && current.is_some() => {
                        current_alias = ArtistAlias {
                            alias_id: get_attr_i32(e, b"id"),
                            name: String::new(),
                        };
                    }
                    b"images" | b"urls" | b"members" | b"groups" => {
                        skip_depth = 1;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                if skip_depth > 0 {
                    skip_depth -= 1;
                    if skip_depth == 0 { section = Section::Root; }
                    text_buf.clear();
                    buf.clear();
                    continue;
                }
                let tag = e.name();
                let text = std::mem::take(&mut text_buf);
                match tag.as_ref() {
                    b"artist" => {
                        if let Some(artist) = current.take() {
                            on_entity(artist);
                            count += 1;
                        }
                    }
                    b"namevariations" => section = Section::Root,
                    b"aliases" => section = Section::Root,
                    _ if current.is_some() => {
                        let artist = current.as_mut().unwrap();
                        match section {
                            Section::Root => match tag.as_ref() {
                                b"id" => artist.id = text.parse().unwrap_or(0),
                                b"name" => artist.name = text,
                                b"realname" => artist.realname = text,
                                b"profile" => artist.profile = text,
                                b"data_quality" => artist.data_quality = text,
                                _ => {}
                            },
                            Section::NameVars => {
                                if tag.as_ref() == b"name" && !text.is_empty() {
                                    artist.namevariations.push(text);
                                }
                            }
                            Section::Aliases => {
                                if tag.as_ref() == b"name" {
                                    current_alias.name = text;
                                    artist.aliases.push(std::mem::take(&mut current_alias));
                                }
                            }

                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                if skip_depth > 0 { buf.clear(); continue; }
                if let Ok(t) = e.unescape() {
                    text_buf.push_str(&t);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(e.into()),
            _ => {}
        }
        buf.clear();
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

pub fn parse_labels(
    reader: impl BufRead,
    mut on_entity: impl FnMut(Label),
) -> anyhow::Result<usize> {
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut count = 0usize;
    let mut current: Option<Label> = None;
    let mut text_buf = String::new();
    let mut skip_depth = 0u32;
    let mut parent_label_pending = false;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                text_buf.clear();
                if skip_depth > 0 { skip_depth += 1; buf.clear(); continue; }
                let tag = e.name();
                match tag.as_ref() {
                    b"label" if current.is_none() => {
                        current = Some(Label::default());
                    }
                    b"parentLabel" if current.is_some() => {
                        let id = get_attr_i32(e, b"id");
                        if id != 0 {
                            current.as_mut().unwrap().parent_label_id = Some(id);
                        }
                        parent_label_pending = true;
                    }
                    b"images" | b"urls" | b"sublabels" => {
                        skip_depth = 1;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                if skip_depth > 0 {
                    skip_depth -= 1;
                    text_buf.clear();
                    buf.clear();
                    continue;
                }
                let tag = e.name();
                let text = std::mem::take(&mut text_buf);
                match tag.as_ref() {
                    b"label" => {
                        if let Some(label) = current.take() {
                            on_entity(label);
                            count += 1;
                        }
                    }
                    b"parentLabel" => { parent_label_pending = false; }
                    _ if current.is_some() && !parent_label_pending => {
                        let label = current.as_mut().unwrap();
                        match tag.as_ref() {
                            b"id" => label.id = text.parse().unwrap_or(0),
                            b"name" => label.name = text,
                            b"contactinfo" => label.contactinfo = text,
                            b"profile" => label.profile = text,
                            b"data_quality" => label.data_quality = text,
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                if skip_depth > 0 { buf.clear(); continue; }
                if let Ok(t) = e.unescape() {
                    text_buf.push_str(&t);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(e.into()),
            _ => {}
        }
        buf.clear();
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Masters
// ---------------------------------------------------------------------------

pub fn parse_masters(
    reader: impl BufRead,
    mut on_entity: impl FnMut(Master),
) -> anyhow::Result<usize> {
    #[derive(PartialEq)]
    enum Section { Root, Artists, InArtist }

    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut count = 0usize;
    let mut current: Option<Master> = None;
    let mut section = Section::Root;
    let mut current_artist = CreditedArtist::default();
    let mut text_buf = String::new();
    let mut skip_depth = 0u32;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                text_buf.clear();
                if skip_depth > 0 { skip_depth += 1; buf.clear(); continue; }
                let tag = e.name();
                match (tag.as_ref(), &section) {
                    (b"master", _) if current.is_none() => {
                        let mut m = Master::default();
                        m.id = get_attr_i32(e, b"id");
                        current = Some(m);
                        section = Section::Root;
                    }
                    (b"artists", Section::Root) if current.is_some() => {
                        section = Section::Artists;
                    }
                    (b"artist", Section::Artists) => {
                        current_artist = CreditedArtist::default();
                        section = Section::InArtist;
                    }
                    (b"images" | b"genres" | b"styles" | b"videos" | b"urls", _) if current.is_some() => {
                        skip_depth = 1;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                if skip_depth > 0 {
                    skip_depth -= 1;
                    text_buf.clear();
                    buf.clear();
                    continue;
                }
                let tag = e.name();
                let text = std::mem::take(&mut text_buf);
                match (tag.as_ref(), &section) {
                    (b"master", _) => {
                        if let Some(master) = current.take() {
                            on_entity(master);
                            count += 1;
                        }
                        section = Section::Root;
                    }
                    (b"artist", Section::InArtist) => {
                        if let Some(ref mut master) = current {
                            master.artists.push(std::mem::take(&mut current_artist));
                        }
                        section = Section::Artists;
                    }
                    (b"artists", Section::Artists) => {
                        section = Section::Root;
                    }
                    (_, Section::InArtist) => {
                        parse_artist_field(tag.as_ref(), &text, &mut current_artist);
                    }
                    (_, Section::Root) if current.is_some() => {
                        let master = current.as_mut().unwrap();
                        match tag.as_ref() {
                            b"title" => master.title = text,
                            b"year" => master.year = text.parse().ok(),
                            b"main_release" => master.main_release_id = text.parse().ok(),
                            b"data_quality" => master.data_quality = text,
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                if skip_depth > 0 { buf.clear(); continue; }
                if let Ok(t) = e.unescape() {
                    text_buf.push_str(&t);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(e.into()),
            _ => {}
        }
        buf.clear();
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Releases
// ---------------------------------------------------------------------------

pub fn parse_releases(
    reader: impl BufRead,
    mut on_entity: impl FnMut(Release),
) -> anyhow::Result<usize> {
    #[derive(Clone, Copy, PartialEq)]
    enum S {
        Root, Artists, InArtist, Labels, Formats, InFormat, InFormatDescs,
        Genres, Styles, Tracklist, InTrack, TrackArtists, InTrackArtist,
        Identifiers, Skip,
    }

    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut count = 0usize;

    let mut current: Option<Release> = None;
    let mut section = S::Root;
    let mut prev_section = S::Root;
    let mut skip_depth = 0u32;

    let mut cur_artist = CreditedArtist::default();
    let mut cur_track = ReleaseTrack::default();
    let mut cur_format = ReleaseFormat::default();
    let mut format_descs: Vec<String> = Vec::new();
    let mut track_seq = 0i32;
    let mut text_buf = String::new();

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                text_buf.clear();
                if skip_depth > 0 { skip_depth += 1; buf.clear(); continue; }
                let tag = e.name();
                match (tag.as_ref(), section) {
                    // Entity start
                    (b"release", _) if current.is_none() => {
                        let mut r = Release::default();
                        r.id = get_attr_i32(e, b"id");
                        r.status = get_attr(e, b"status");
                        current = Some(r);
                        section = S::Root;
                        track_seq = 0;
                    }
                    // Section openers from Root
                    (b"artists", S::Root) => section = S::Artists,
                    (b"labels", S::Root) => section = S::Labels,
                    (b"formats", S::Root) => section = S::Formats,
                    (b"genres", S::Root) => section = S::Genres,
                    (b"styles", S::Root) => section = S::Styles,
                    (b"tracklist", S::Root) => section = S::Tracklist,
                    (b"identifiers", S::Root) => section = S::Identifiers,
                    // Skip sections
                    (b"images" | b"videos" | b"companies" | b"extraartists" | b"urls", s) => {
                        prev_section = s;
                        section = S::Skip;
                        skip_depth = 1;
                    }
                    // Artists subsection
                    (b"artist", S::Artists) => {
                        cur_artist = CreditedArtist::default();
                        section = S::InArtist;
                    }
                    // Formats subsection
                    (b"format", S::Formats) => {
                        cur_format = ReleaseFormat {
                            name: get_attr(e, b"name"),
                            qty: get_attr(e, b"qty").parse().unwrap_or(1),
                            free_text: get_attr(e, b"text"),
                            ..Default::default()
                        };
                        format_descs.clear();
                        section = S::InFormat;
                    }
                    (b"descriptions", S::InFormat) => section = S::InFormatDescs,
                    // Tracklist subsection
                    (b"track", S::Tracklist) => {
                        track_seq += 1;
                        cur_track = ReleaseTrack { sequence: track_seq, ..Default::default() };
                        section = S::InTrack;
                    }
                    (b"artists", S::InTrack) => section = S::TrackArtists,
                    (b"artist", S::TrackArtists) => {
                        cur_artist = CreditedArtist::default();
                        section = S::InTrackArtist;
                    }
                    // extraartists inside track handled by the broader skip match above
                    _ => {}
                }
            }
            Ok(Event::Empty(ref e)) => {
                if skip_depth > 0 { buf.clear(); continue; }
                if current.is_none() { buf.clear(); continue; }
                let release = current.as_mut().unwrap();
                let tag = e.name();
                match (tag.as_ref(), section) {
                    (b"label", S::Labels) => {
                        release.labels.push(ReleaseLabel {
                            label_id: get_attr_i32(e, b"id"),
                            label_name: get_attr(e, b"name"),
                            catno: get_attr(e, b"catno"),
                        });
                    }
                    (b"identifier", S::Identifiers) => {
                        release.identifiers.push(ReleaseIdentifier {
                            type_: get_attr(e, b"type"),
                            value: get_attr(e, b"value"),
                            description: get_attr(e, b"description"),
                        });
                    }
                    (b"format", S::Formats) => {
                        release.formats.push(ReleaseFormat {
                            name: get_attr(e, b"name"),
                            qty: get_attr(e, b"qty").parse().unwrap_or(1),
                            free_text: get_attr(e, b"text"),
                            descriptions: String::new(),
                        });
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                if skip_depth > 0 {
                    skip_depth -= 1;
                    if skip_depth == 0 { section = prev_section; }
                    text_buf.clear();
                    buf.clear();
                    continue;
                }
                let tag = e.name();
                let text = std::mem::take(&mut text_buf);
                match (tag.as_ref(), section) {
                    // Entity end
                    (b"release", _) => {
                        if let Some(release) = current.take() {
                            on_entity(release);
                            count += 1;
                        }
                        section = S::Root;
                    }
                    // Section closers
                    (b"artists", S::Artists) => section = S::Root,
                    (b"labels", S::Labels) => section = S::Root,
                    (b"formats", S::Formats) => section = S::Root,
                    (b"genres", S::Genres) => section = S::Root,
                    (b"styles", S::Styles) => section = S::Root,
                    (b"tracklist", S::Tracklist) => section = S::Root,
                    (b"identifiers", S::Identifiers) => section = S::Root,
                    // Artist end
                    (b"artist", S::InArtist) => {
                        if let Some(ref mut r) = current {
                            r.artists.push(std::mem::take(&mut cur_artist));
                        }
                        section = S::Artists;
                    }
                    // Artist fields
                    (_, S::InArtist) => {
                        parse_artist_field(tag.as_ref(), &text, &mut cur_artist);
                    }
                    // Format end
                    (b"format", S::InFormat | S::InFormatDescs) => {
                        cur_format.descriptions = format_descs.join(", ");
                        if let Some(ref mut r) = current {
                            r.formats.push(std::mem::take(&mut cur_format));
                        }
                        section = S::Formats;
                    }
                    (b"descriptions", S::InFormatDescs) => section = S::InFormat,
                    (b"description", S::InFormatDescs) => {
                        if !text.is_empty() {
                            format_descs.push(text);
                        }
                    }
                    // Genre/style text
                    (b"genre", S::Genres) => {
                        if let Some(ref mut r) = current {
                            if !text.is_empty() { r.genres.push(text); }
                        }
                    }
                    (b"style", S::Styles) => {
                        if let Some(ref mut r) = current {
                            if !text.is_empty() { r.styles.push(text); }
                        }
                    }
                    // Track artist end
                    (b"artist", S::InTrackArtist) => {
                        cur_track.artists.push(std::mem::take(&mut cur_artist));
                        section = S::TrackArtists;
                    }
                    (_, S::InTrackArtist) => {
                        parse_artist_field(tag.as_ref(), &text, &mut cur_artist);
                    }
                    // Track artists section close
                    (b"artists", S::TrackArtists) => section = S::InTrack,
                    // Track end
                    (b"track", S::InTrack) => {
                        if let Some(ref mut r) = current {
                            r.tracks.push(std::mem::take(&mut cur_track));
                        }
                        section = S::Tracklist;
                    }
                    // Track fields
                    (_, S::InTrack) => match tag.as_ref() {
                        b"position" => cur_track.position = text,
                        b"title" => cur_track.title = text,
                        b"duration" => cur_track.duration = text,
                        _ => {}
                    },
                    // Root-level release fields
                    (_, S::Root) if current.is_some() => {
                        let r = current.as_mut().unwrap();
                        match tag.as_ref() {
                            b"title" => r.title = text,
                            b"country" => r.country = text,
                            b"released" => r.released = text,
                            b"notes" => r.notes = text,
                            b"data_quality" => r.data_quality = text,
                            b"master_id" => r.master_id = text.parse().ok(),
                            _ => {}
                        }
                    }
                    // Labels handled as Empty events; End("label") is a no-op
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                if skip_depth > 0 { buf.clear(); continue; }
                if let Ok(t) = e.unescape() {
                    text_buf.push_str(&t);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(e.into()),
            _ => {}
        }
        buf.clear();
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_artist() {
        let xml = br#"
        <artists>
          <artist>
            <id>1</id>
            <name>The Persuader</name>
            <realname>Jesper Dahlb&#228;ck</realname>
            <profile>Swedish producer</profile>
            <data_quality>Correct</data_quality>
            <namevariations>
              <name>Persuader</name>
              <name>The Presuader</name>
            </namevariations>
            <aliases>
              <name id="2">Dick Track</name>
            </aliases>
          </artist>
        </artists>
        "#;
        let mut artists = Vec::new();
        let count = parse_artists(&xml[..], |a| artists.push(a)).unwrap();
        assert_eq!(count, 1);
        let a = &artists[0];
        assert_eq!(a.id, 1);
        assert_eq!(a.name, "The Persuader");
        assert_eq!(a.realname, "Jesper Dahlbäck");
        assert_eq!(a.namevariations, vec!["Persuader", "The Presuader"]);
        assert_eq!(a.aliases.len(), 1);
        assert_eq!(a.aliases[0].alias_id, 2);
        assert_eq!(a.aliases[0].name, "Dick Track");
    }

    #[test]
    fn test_parse_label() {
        let xml = br#"
        <labels>
          <label>
            <id>1</id>
            <name>Planet E</name>
            <contactinfo>Detroit</contactinfo>
            <profile>Founded by Carl Craig</profile>
            <data_quality>Correct</data_quality>
            <parentLabel id="999">Parent</parentLabel>
          </label>
        </labels>
        "#;
        let mut labels = Vec::new();
        let count = parse_labels(&xml[..], |l| labels.push(l)).unwrap();
        assert_eq!(count, 1);
        assert_eq!(labels[0].id, 1);
        assert_eq!(labels[0].name, "Planet E");
        assert_eq!(labels[0].parent_label_id, Some(999));
    }

    #[test]
    fn test_parse_master() {
        let xml = br#"
        <masters>
          <master id="123">
            <title>OK Computer</title>
            <year>1997</year>
            <main_release>456</main_release>
            <data_quality>Correct</data_quality>
            <artists>
              <artist>
                <id>1</id>
                <name>Radiohead</name>
                <anv></anv>
                <role></role>
                <join>,</join>
              </artist>
            </artists>
          </master>
        </masters>
        "#;
        let mut masters = Vec::new();
        let count = parse_masters(&xml[..], |m| masters.push(m)).unwrap();
        assert_eq!(count, 1);
        assert_eq!(masters[0].id, 123);
        assert_eq!(masters[0].title, "OK Computer");
        assert_eq!(masters[0].year, Some(1997));
        assert_eq!(masters[0].main_release_id, Some(456));
        assert_eq!(masters[0].artists.len(), 1);
        assert_eq!(masters[0].artists[0].artist_name, "Radiohead");
    }

    #[test]
    fn test_parse_release() {
        let xml = br#"
        <releases>
          <release id="1" status="Accepted">
            <title>Stockholm</title>
            <country>Sweden</country>
            <released>1999</released>
            <notes>Classic</notes>
            <data_quality>Correct</data_quality>
            <master_id>123</master_id>
            <artists>
              <artist>
                <id>1</id>
                <name>The Persuader</name>
                <anv></anv>
                <role></role>
                <join>,</join>
              </artist>
            </artists>
            <labels>
              <label catno="SVK001" id="5" name="Svek"/>
            </labels>
            <formats>
              <format name="Vinyl" qty="2" text="">
                <descriptions>
                  <description>12&quot;</description>
                  <description>33 &#x2153; RPM</description>
                </descriptions>
              </format>
            </formats>
            <genres>
              <genre>Electronic</genre>
            </genres>
            <styles>
              <style>Deep House</style>
            </styles>
            <tracklist>
              <track>
                <position>A</position>
                <title>Ostermalm</title>
                <duration>6:13</duration>
              </track>
              <track>
                <position>B</position>
                <title>Vasaansen</title>
                <duration>5:20</duration>
                <artists>
                  <artist>
                    <id>2</id>
                    <name>Other Artist</name>
                    <anv></anv>
                    <role></role>
                    <join></join>
                  </artist>
                </artists>
              </track>
            </tracklist>
            <identifiers>
              <identifier type="Barcode" value="123456" description=""/>
            </identifiers>
          </release>
        </releases>
        "#;
        let mut releases = Vec::new();
        let count = parse_releases(&xml[..], |r| releases.push(r)).unwrap();
        assert_eq!(count, 1);
        let r = &releases[0];
        assert_eq!(r.id, 1);
        assert_eq!(r.status, "Accepted");
        assert_eq!(r.title, "Stockholm");
        assert_eq!(r.country, "Sweden");
        assert_eq!(r.master_id, Some(123));
        assert_eq!(r.artists.len(), 1);
        assert_eq!(r.artists[0].artist_name, "The Persuader");
        assert_eq!(r.labels.len(), 1);
        assert_eq!(r.labels[0].catno, "SVK001");
        assert_eq!(r.formats.len(), 1);
        assert_eq!(r.formats[0].name, "Vinyl");
        assert_eq!(r.formats[0].qty, 2);
        assert!(r.formats[0].descriptions.contains("12\""));
        assert_eq!(r.genres, vec!["Electronic"]);
        assert_eq!(r.styles, vec!["Deep House"]);
        assert_eq!(r.tracks.len(), 2);
        assert_eq!(r.tracks[0].position, "A");
        assert_eq!(r.tracks[0].sequence, 1);
        assert_eq!(r.tracks[1].artists.len(), 1);
        assert_eq!(r.identifiers.len(), 1);
        assert_eq!(r.identifiers[0].type_, "Barcode");
        assert_eq!(r.identifiers[0].value, "123456");
    }
}
