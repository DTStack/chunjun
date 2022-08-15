const app = require('express')()
const path = require('path');
app.use(require('express').static('./'));
app.use('/chunjun', (req, res) => {
    res.sendFile('index.html', {
        root: path.join(__dirname, './')
    });
});

app.listen(5010);