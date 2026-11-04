use std::borrow::Cow;

use super::Error;

/// Borrowed Cargo JSON; only extracted escaped strings need owned storage.
#[derive(Clone, Copy, Debug)]
pub(super) struct Value<'a> {
    pub(super) text: &'a str,
    offset: usize,
}

impl<'a> Value<'a> {
    pub(super) fn parse(text: &'a str) -> Result<Self, Error> {
        let mut cursor = Cursor {
            rest: text,
            offset: 0,
        };
        let value = cursor.take(0)?;
        cursor.space();
        if !cursor.rest.is_empty() {
            return Err(cursor.error());
        }
        Ok(value)
    }

    pub(super) fn field(self, name: &'static str) -> Result<Self, Error> {
        let mut cursor = self.contents(b'{', b'}')?;
        while !cursor.rest.trim().is_empty() {
            let key = cursor.take(0)?;
            cursor.expect(b':')?;
            let value = cursor.take(0)?;
            if key.string()? == name {
                return Ok(value);
            }
            if !cursor.rest.trim().is_empty() {
                cursor.expect(b',')?;
            }
        }
        Err(Error::Field(name))
    }

    pub(super) fn array(self) -> Result<Vec<Self>, Error> {
        let mut cursor = self.contents(b'[', b']')?;
        let mut values = Vec::new();
        while !cursor.rest.trim().is_empty() {
            values.push(cursor.take(0)?);
            if !cursor.rest.trim().is_empty() {
                cursor.expect(b',')?;
            }
        }
        Ok(values)
    }

    fn contents(self, open: u8, close: u8) -> Result<Cursor<'a>, Error> {
        if self.text.as_bytes().first() != Some(&open)
            || self.text.as_bytes().last() != Some(&close)
        {
            return Err(Error::Message {
                offset: self.offset,
            });
        }
        Ok(Cursor {
            rest: &self.text[1..self.text.len() - 1],
            offset: self.offset + 1,
        })
    }

    pub(super) fn string(&self) -> Result<Cow<'a, str>, Error> {
        let cursor = self.contents(b'"', b'"')?;
        if !cursor.rest.contains('\\') {
            return Ok(Cow::Borrowed(cursor.rest));
        }
        let mut output = String::new();
        self.decode(|ch| output.push(ch))?;
        Ok(Cow::Owned(output))
    }

    fn decode(&self, mut emit: impl FnMut(char)) -> Result<(), Error> {
        let cursor = self.contents(b'"', b'"')?;
        if cursor.rest.bytes().any(|byte| byte < 0x20) {
            return Err(cursor.error());
        }
        let mut chars = cursor.rest.chars();
        while let Some(ch) = chars.next() {
            emit(if ch == '\\' {
                match chars.next().ok_or_else(|| cursor.error())? {
                    '"' => '"',
                    '\\' => '\\',
                    '/' => '/',
                    'b' => '\u{8}',
                    'f' => '\u{c}',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'u' => {
                        let mut code = hex(&mut chars).ok_or_else(|| cursor.error())?;
                        if (0xd800..=0xdbff).contains(&code) {
                            if chars.next() != Some('\\') || chars.next() != Some('u') {
                                return Err(cursor.error());
                            }
                            let low = hex(&mut chars)
                                .filter(|low| (0xdc00..=0xdfff).contains(low))
                                .ok_or_else(|| cursor.error())?;
                            code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
                        }
                        char::from_u32(code).ok_or_else(|| cursor.error())?
                    }
                    _ => return Err(cursor.error()),
                }
            } else {
                ch
            });
        }
        Ok(())
    }
}

fn hex(chars: &mut std::str::Chars<'_>) -> Option<u32> {
    (0..4).try_fold(0, |value, _| {
        Some((value << 4) | chars.next()?.to_digit(16)?)
    })
}

#[derive(Debug)]
struct Cursor<'a> {
    rest: &'a str,
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn error(&self) -> Error {
        Error::Message {
            offset: self.offset,
        }
    }

    fn advance(&mut self, bytes: usize) {
        self.rest = &self.rest[bytes..];
        self.offset += bytes;
    }

    fn space(&mut self) {
        let count = self
            .rest
            .bytes()
            .take_while(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
            .count();
        self.advance(count);
    }

    fn expect(&mut self, byte: u8) -> Result<(), Error> {
        self.space();
        if self.rest.as_bytes().first() != Some(&byte) {
            return Err(self.error());
        }
        self.advance(1);
        Ok(())
    }

    fn take(&mut self, depth: u8) -> Result<Value<'a>, Error> {
        self.space();
        let start = self.rest;
        let offset = self.offset;
        match self.rest.as_bytes().first().copied() {
            Some(b'"') => {
                let mut escaped = false;
                let end = self.rest.as_bytes()[1..]
                    .iter()
                    .position(|&byte| {
                        if escaped {
                            escaped = false;
                            false
                        } else if byte == b'\\' {
                            escaped = true;
                            false
                        } else {
                            byte == b'"'
                        }
                    })
                    .ok_or_else(|| self.error())?
                    + 2;
                self.advance(end);
                Value {
                    text: &start[..end],
                    offset,
                }
                .decode(drop)?;
            }
            Some(open @ (b'[' | b'{')) if depth < 64 => {
                let close = if open == b'[' { b']' } else { b'}' };
                self.advance(1);
                self.space();
                if self.rest.as_bytes().first() == Some(&close) {
                    self.advance(1);
                } else {
                    loop {
                        if open == b'{' {
                            self.take(depth + 1)?.contents(b'"', b'"')?;
                            self.expect(b':')?;
                        }
                        self.take(depth + 1)?;
                        self.space();
                        if self.rest.as_bytes().first() == Some(&close) {
                            self.advance(1);
                            break;
                        }
                        self.expect(b',')?;
                    }
                }
            }
            Some(_) => {
                let count = self
                    .rest
                    .bytes()
                    .take_while(|byte| !byte.is_ascii_whitespace() && !b",]}:".contains(byte))
                    .count();
                let atom = &self.rest[..count];
                if !matches!(atom, "true" | "false" | "null") && !number(atom) {
                    return Err(self.error());
                }
                self.advance(count);
            }
            None => return Err(self.error()),
        }
        Ok(Value {
            text: &start[..self.offset - offset],
            offset,
        })
    }
}

fn number(text: &str) -> bool {
    let text = text.strip_prefix('-').unwrap_or(text);
    let integer = text.bytes().take_while(u8::is_ascii_digit).count();
    if integer == 0 || integer > 1 && text.starts_with('0') {
        return false;
    }
    let mut rest = &text[integer..];
    if let Some(fraction) = rest.strip_prefix('.') {
        let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return false;
        }
        rest = &fraction[digits..];
    }
    if let Some(exponent) = rest.strip_prefix(['e', 'E']) {
        let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        return !exponent.is_empty() && exponent.bytes().all(|byte| byte.is_ascii_digit());
    }
    rest.is_empty()
}
