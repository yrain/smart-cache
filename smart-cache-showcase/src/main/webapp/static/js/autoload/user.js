new Vue({
    el: '.container',
    data() {
        return {
            loading:false,
            users: []
        };
    },
    mounted() {
        const vm = this;
        vm.refresh();
    },
    methods: {
        create() {
            const vm = this;
            vm.$create().then(data => {
                if (data === null) {
                    alert('添加失败!');
                } else {
                    vm.refresh();
                }
            });
        },
        refresh() {
            const vm = this;
            vm.$find().then(data => {
                vm.users = data;
            });
        },
        get(id) {
            const vm = this;
            vm.$get(id).then(data => {
                if (data === null) {
                    alert('获取数据失败!');
                } else {
                    alert(JSON.stringify(data));
                }
            })
        },
        del(id) {
            const vm = this;
            let isDel = confirm('确认删除?');
            if (!isDel) {
                return;
            }
            vm.$delete(id).then(success => {
                if (success) {
                    vm.refresh();
                } else {
                    alert('删除失败!');
                }
            })
        },
        $find() {
            const vm = this;
            vm.loading = true;
            return Promise.all([fetch(`/showcase/autoload/user/find`).then((res) => {
                return res.json();
            })]).then(arr => {
                vm.loading = false;
                return Promise.resolve(arr[0]);
            });
        },
        $get(id) {
            return new Promise(resolve => {
                fetch(`/showcase/autoload/user/get?id=${id}`).then((res) => {
                    res.json().then((data) => resolve(data));
                }).catch(() => {
                    alert('网络错误');
                    resolve(null);
                })
            });
        },
        $create() {
            return new Promise(resolve => {
                fetch(`/showcase/autoload/user/create`).then((res) => {
                    res.json().then((data) => resolve(data));
                }).catch(() => {
                    alert('网络错误');
                    resolve(null);
                })
            });
        },
        $delete(id) {
            return new Promise(resolve => {
                fetch(`/showcase/autoload/user/delete?id=${id}`).then(() => resolve(true)).catch(() => resolve(false))
            });
        }
    }
});