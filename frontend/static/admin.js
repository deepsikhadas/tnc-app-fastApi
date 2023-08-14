
function get_url(target){
	stage = $("body").data("stage")
	if(stage == ""){
		return target
	}else{
		return "/"+stage+target
	}
}

const { createApp } = Vue

async function doAjax(url, data){
	return $.ajax(
		{url:get_url(url), 
		data:data})
}


app = createApp({
	data:function(){
		return{
			userlist:[]
		}
	},
	template:`<div>
		<user_block
			v-for="user in userlist"
			:user='user'
			:key="user.job_id">
		</user_block>
	</div>`,
	mounted:function(){
		this.get_items()
	},
	methods:{
		get_items:async function(){	
			items = await doAjax("/all_users")
			this.userlist = items.items
		}
	}
}).component("user_block",{
	props:["user"],
	template:`<div class="user_block"> 
		<div class="user_name">{{user.user_email}}</div>
		<div class="user_attr">
			Is Active: <input type="checkbox" v-model="user.active" @change="update" :disabled = "user.admin == true">
		</div>
		<div class="user_attr">
			Is Admin: <input type="checkbox" v-model="user.admin" @change="update" :disabled = "user.admin == true">
		</div>
		</div>`,
	methods:{
		update:function(){
			console.log("update")
			console.log(this.user.job_id)
			console.log(this.user.active)
			$.ajax({
				url:get_url("/update_user"), 
				data:{uid:this.user.job_id, active:this.user.active, admin:this.user.admin, email:this.user.user_email}
			})
		}
	}
})



app.mount("#user_list")