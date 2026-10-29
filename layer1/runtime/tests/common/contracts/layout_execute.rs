fn execute_shape<'app, 'life: 'app, 'd: 'life>(
    app: session::App<'app, 'life, 'd, LayoutOnly>,
) -> Result<shutdown::Requested<()>, drive::Error> {
    app.run()
}
