use syn::{parse_quote, visit_mut, visit_mut::VisitMut as _};

pub(crate) struct Receiver {
    ty: syn::TypePath,
}

impl Receiver {
    /// Keeps the declared owner when copied syntax enters generated helper types.
    pub(crate) fn apply(input: &mut syn::DeriveInput) {
        let name = &input.ident;
        let args = input.generics.split_for_impl().1;
        let mut ty: syn::TypePath = parse_quote!(#name #args);
        for segment in &mut ty.path.segments {
            if let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments {
                args.colon2_token = Some(Default::default());
            }
        }
        Self { ty }.visit_derive_input_mut(input);
    }

    fn qualify(&self, qself: &mut Option<syn::QSelf>, path: &mut syn::Path) {
        if qself.is_some() || path.leading_colon.is_some() || path.segments.len() < 2 {
            return;
        }
        if !path
            .segments
            .first()
            .is_some_and(|part| part.ident == "Self" && part.arguments.is_empty())
        {
            return;
        }
        path.segments = path.segments.iter().skip(1).cloned().collect();
        path.leading_colon = Some(Default::default());
        *qself = Some(syn::QSelf {
            lt_token: Default::default(),
            ty: Box::new(syn::Type::Path(self.ty.clone())),
            position: 0,
            as_token: None,
            gt_token: Default::default(),
        });
    }
}

impl visit_mut::VisitMut for Receiver {
    fn visit_type_path_mut(&mut self, ty: &mut syn::TypePath) {
        self.qualify(&mut ty.qself, &mut ty.path);
        visit_mut::visit_type_path_mut(self, ty);
    }

    fn visit_expr_path_mut(&mut self, expr: &mut syn::ExprPath) {
        self.qualify(&mut expr.qself, &mut expr.path);
        visit_mut::visit_expr_path_mut(self, expr);
    }

    fn visit_path_mut(&mut self, path: &mut syn::Path) {
        if path.is_ident("Self") {
            *path = self.ty.path.clone();
        } else {
            visit_mut::visit_path_mut(self, path);
        }
    }

    /// Nested declarations establish their own receiver and generic scope.
    fn visit_stmt_mut(&mut self, stmt: &mut syn::Stmt) {
        if !matches!(stmt, syn::Stmt::Item(_)) {
            visit_mut::visit_stmt_mut(self, stmt);
        }
    }
}
