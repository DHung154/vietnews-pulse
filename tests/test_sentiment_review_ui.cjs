// No browser or real human labels: run against a generated synthetic fixture only.
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const page = fs.readFileSync(process.argv[2], 'utf8');
const scripts = [...page.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)];
const bundle = JSON.parse(scripts[0][1]);
assert.equal(bundle.records.length, 1);
assert.equal(bundle.records[0].annotation_id, 'fixture-a');
const elements = {};
const get = id => elements[id] || (elements[id] = {textContent:'', value:'', checked:false});
get('bundle').textContent = scripts[0][1];
let downloaded;
const sandbox = {
    document: {getElementById: get, createElement: () => ({click(){}})},
    window: {confirm: () => true, addEventListener(){}}, Blob,
    URL: {createObjectURL: b => {downloaded=b; return 'blob:fixture';}, revokeObjectURL(){}},
    setTimeout: f => f(), console,
};
vm.runInNewContext(scripts[1][1], sandbox);
async function main() {
    assert(get('text').textContent.includes('</script>'));
    assert(get('progress').textContent.includes('0/1'));
    get('save').onclick();
    assert(get('message').textContent.includes('Cần'));
    get('reviewer').value = 'SYNTHETIC_TEST_ONLY';
    get('label').value = 'positive';
    get('evidence').value = 'not in text';
    get('reason').value = 'Synthetic fixture only';
    get('confirm').checked = true;
    get('save').onclick();
    assert(get('message').textContent.includes('nguyên văn'));
    get('evidence').value = 'Thành công';
    get('save').onclick();
    assert(get('progress').textContent.includes('1/1'));
    get('download').onclick();
    const exported = JSON.parse(await downloaded.text());
    assert.equal(exported.mode, 'assisted');
    assert.equal(exported.reviews.length, 1);
    assert.equal(exported.reviews[0].reviewer, 'SYNTHETIC_TEST_ONLY');
    assert(!exported.reviews[0].human_gold);
    get('restore').files = [{text: async () => JSON.stringify({...exported, mode:'blind'})}];
    await get('restore').onchange();
    assert(get('message').textContent.includes('Không nạp file'));
    get('restore').files = [{text: async () => JSON.stringify(exported)}];
    await get('restore').onchange();
    assert(get('message').textContent.includes('Đã nạp'));
    assert.equal(get('label').value, 'positive');
    assert.equal(get('confirm').checked, false);
    console.log('Synthetic JS smoke passed: validation/save/progress/export/restore. Not a browser layout test.');
}
main().catch(error => {console.error(error);process.exitCode=1;});
