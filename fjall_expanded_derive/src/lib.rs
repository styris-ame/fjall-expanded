use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, GenericParam, parse_macro_input};

#[proc_macro_derive(SpaceKey)]
pub fn derive_store_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Always produce a clean path using Ident (handles rename + internal use)
    let fjall_expanded_path: proc_macro2::TokenStream = match proc_macro_crate::crate_name("fjall_expanded") {
        Ok(proc_macro_crate::FoundCrate::Itself) => quote! { crate },
        Ok(proc_macro_crate::FoundCrate::Name(name)) => {
            let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote! { ::#ident }
        }
        Err(_) => {
            let ident = syn::Ident::new("fjall_expanded", proc_macro2::Span::call_site());
            quote! { ::#ident }
        }
    };

    let ident = &input.ident;
    let generics = &input.generics;

    let non_lifetime_params: Vec<_> = generics.params.iter().filter(|p| !matches!(p, GenericParam::Lifetime(_))).collect();

    let impl_generics = if non_lifetime_params.is_empty() {
        quote! {}
    } else {
        quote! { < #(#non_lifetime_params),* > }
    };

    let where_clause = &generics.where_clause;

    let build_args = |replacement: proc_macro2::TokenStream| -> proc_macro2::TokenStream {
        if generics.params.is_empty() {
            return quote! {};
        }
        let args: Vec<_> = generics
            .params
            .iter()
            .map(|param| match param {
                GenericParam::Lifetime(_) => replacement.clone(),
                GenericParam::Type(t) => {
                    let ident = &t.ident;
                    quote! { #ident }
                }
                GenericParam::Const(c) => {
                    let ident = &c.ident;
                    quote! { #ident }
                }
            })
            .collect();
        quote! { < #(#args),* > }
    };

    let marker_args = build_args(quote! { '_ });
    let borrowed_args = build_args(quote! { 'a });

    let expanded = quote! {
        impl #impl_generics #fjall_expanded_path::Key for #ident #marker_args #where_clause {
            type Borrowed<'a> = #ident #borrowed_args;
        }
    };

    TokenStream::from(expanded)
}
