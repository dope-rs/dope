use proc_macro2::{Span, TokenStream, TokenTree};

use crate::Failure;

pub(crate) fn ident(source: &TokenStream, name: &str) -> syn::Ident {
    let mut name = name.to_owned();
    while contains(source.clone(), &name) {
        name.push('_');
    }
    syn::Ident::new(&name, Span::mixed_site())
}

pub(crate) fn lifetime(source: &TokenStream, name: &str) -> syn::Lifetime {
    let name = ident(source, name);
    syn::Lifetime::new(&format!("'{name}"), name.span())
}

fn contains(source: TokenStream, name: &str) -> bool {
    source.into_iter().any(|token| match token {
        TokenTree::Ident(ident) => {
            let ident = ident.to_string();
            ident.strip_prefix("r#").unwrap_or(&ident) == name
        }
        TokenTree::Group(group) => contains(group.stream(), name),
        _ => false,
    })
}

pub(crate) struct Code {
    pub(crate) tokens: TokenStream,
    pub(crate) span: Span,
}

pub(crate) struct Value {
    pub(crate) name: syn::Ident,
    pub(crate) value: Code,
}

pub(crate) fn values(
    list: &syn::MetaList,
    fail: impl Fn(Span) -> Failure,
) -> Result<Vec<Value>, Failure> {
    let tokens: Vec<_> = list.tokens.clone().into_iter().collect();
    let mut values = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let TokenTree::Ident(name) = &tokens[index] else {
            return Err(fail(tokens[index].span()));
        };
        index += 1;
        let Some(TokenTree::Punct(equal)) = tokens.get(index) else {
            return Err(fail(name.span()));
        };
        if equal.as_char() != '=' {
            return Err(fail(equal.span()));
        }
        index += 1;
        let start = index;
        while index < tokens.len()
            && !matches!(&tokens[index], TokenTree::Punct(mark) if mark.as_char() == ',')
        {
            index += 1;
        }
        if start == index {
            return Err(fail(name.span()));
        }
        values.push(Value {
            name: name.clone(),
            value: Code {
                tokens: tokens[start..index].iter().cloned().collect(),
                span: tokens[start].span(),
            },
        });
        index += usize::from(index < tokens.len());
    }
    Ok(values)
}
